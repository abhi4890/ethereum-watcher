package ethereum_watcher

import (
	"context"
	"fmt"
	"github.com/HydroProtocol/ethereum-watcher/blockchain"
	"github.com/HydroProtocol/ethereum-watcher/rpc"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type ReceiptLogWatcherV2 struct {
	ctx                   context.Context
	api                   string
	startBlockNum         int
	contractAddresses     []string
	interestedTopics      []string
	handler               func(from, to int, receiptLogs []blockchain.IReceiptLog, isUpToHighestBlock bool) error
	config                ReceiptLogWatcherV2Config
	highestSyncedBlockNum int
	highestSyncedLogIndex int
}

func NewReceiptLogWatcherV2(
	ctx context.Context,
	api string,
	startBlockNum int,
	contractAddresses []string,
	interestedTopics []string,
	handler func(from, to int, receiptLogs []blockchain.IReceiptLog, isUpToHighestBlock bool) error,
	configs ...ReceiptLogWatcherV2Config,
) *ReceiptLogWatcherV2 {

	config := decideReceiptLogWatcherV2Config(configs...)

	pseudoSyncedLogIndex := config.StartSyncAfterLogIndex - 1

	return &ReceiptLogWatcherV2{
		ctx:                   ctx,
		api:                   api,
		startBlockNum:         startBlockNum,
		contractAddresses:     contractAddresses,
		interestedTopics:      interestedTopics,
		handler:               handler,
		config:                config,
		highestSyncedBlockNum: startBlockNum,
		highestSyncedLogIndex: pseudoSyncedLogIndex,
	}
}

func decideReceiptLogWatcherV2Config(configs ...ReceiptLogWatcherV2Config) ReceiptLogWatcherV2Config {
	var config ReceiptLogWatcherV2Config
	if len(configs) == 0 {
		config = defaultReceiptLogWatcherV2Config
	} else {
		config = configs[0]

		if config.IntervalForPollingNewBlockInSec <= 0 {
			config.IntervalForPollingNewBlockInSec = defaultReceiptLogWatcherV2Config.IntervalForPollingNewBlockInSec
		}

		if config.StepSizeForBigLag <= 0 {
			config.StepSizeForBigLag = defaultReceiptLogWatcherV2Config.StepSizeForBigLag
		}

		if config.RPCMaxRetry <= 0 {
			config.RPCMaxRetry = defaultReceiptLogWatcherV2Config.RPCMaxRetry
		}
	}

	return config
}

type ReceiptLogWatcherV2Config struct {
	StepSizeForBigLag               int
	ReturnForBlockWithNoReceiptLog  bool
	IntervalForPollingNewBlockInSec int
	RPCMaxRetry                     int
	LagToHighestBlock               int
	StartSyncAfterLogIndex          int
}

var defaultReceiptLogWatcherV2Config = ReceiptLogWatcherV2Config{
	StepSizeForBigLag:               50,
	ReturnForBlockWithNoReceiptLog:  false,
	IntervalForPollingNewBlockInSec: 15,
	RPCMaxRetry:                     5,
	LagToHighestBlock:               0,
	StartSyncAfterLogIndex:          0,
}

func (w *ReceiptLogWatcherV2) Run() error {

	var blockNumToBeProcessedNext = w.startBlockNum

	rpc := rpc.NewEthRPCWithRetry(w.api, w.config.RPCMaxRetry)

	for {
		select {
		case <-w.ctx.Done():
			return nil
		default:
			highestBlock, err := rpc.GetCurrentBlockNum()
			if err != nil {
				return err
			}

			if blockNumToBeProcessedNext < 0 {
				blockNumToBeProcessedNext = int(highestBlock)
			}

			highestBlockCanProcess := int(highestBlock) - w.config.LagToHighestBlock
			numOfBlocksToProcess := highestBlockCanProcess - blockNumToBeProcessedNext + 1

			if numOfBlocksToProcess <= 0 {
				sleepSec := w.config.IntervalForPollingNewBlockInSec

				logrus.Debugf("no ready block after %d(lag: %d), sleep %d seconds", highestBlockCanProcess, w.config.LagToHighestBlock, sleepSec)

				select {
				case <-time.After(time.Duration(sleepSec) * time.Second):
					continue
				case <-w.ctx.Done():
					return nil
				}
			}

			var to int
			if numOfBlocksToProcess > w.config.StepSizeForBigLag {
				// quick mode
				to = blockNumToBeProcessedNext + w.config.StepSizeForBigLag - 1
			} else {
				// normal mode, up to cur highest block num can process
				to = highestBlockCanProcess
			}

			logs, err := rpc.GetLogs(uint64(blockNumToBeProcessedNext), uint64(to), w.contractAddresses, w.interestedTopics)
			if err != nil {
				return err
			}

			isUpToHighestBlock := to == int(highestBlock)

			if len(logs) == 0 {
				if w.config.ReturnForBlockWithNoReceiptLog {
					err := w.handler(blockNumToBeProcessedNext, to, nil, isUpToHighestBlock)
					if err != nil {
						logrus.Infof("err when handling nil receipt log, block range: %d - %d", blockNumToBeProcessedNext, to)
						return fmt.Errorf("ethereum_watcher handler(nil) returns error: %s", err)
					}
				}
			} else {

				err := w.handler(blockNumToBeProcessedNext, to, logs, isUpToHighestBlock)
				if err != nil {
					logrus.Infof("err when handling receipt log, block range: %d - %d, receipt logs: %+v",
						blockNumToBeProcessedNext, to, logs,
					)

					return fmt.Errorf("ethereum_watcher handler returns error: %s", err)
				}
			}

			// todo rm 2nd param
			w.updateHighestSyncedBlockNumAndLogIndex(to, -1)

			blockNumToBeProcessedNext = to + 1
		}
	}
}

var progressLock2 = sync.Mutex{}

func (w *ReceiptLogWatcherV2) updateHighestSyncedBlockNumAndLogIndex(block int, logIndex int) {
	progressLock2.Lock()
	defer progressLock2.Unlock()

	w.highestSyncedBlockNum = block
	w.highestSyncedLogIndex = logIndex
}

func (w *ReceiptLogWatcherV2) GetHighestSyncedBlockNum() int {
	return w.highestSyncedBlockNum
}

func (w *ReceiptLogWatcherV2) GetHighestSyncedBlockNumAndLogIndex() (int, int) {
	progressLock2.Lock()
	defer progressLock2.Unlock()

	return w.highestSyncedBlockNum, w.highestSyncedLogIndex
}
