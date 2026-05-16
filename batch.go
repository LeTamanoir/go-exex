package indexer

import (
	"cmp"
	"context"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

func (idx *Indexer) fetchAndProcessLogs(ctx context.Context, fromBlock, toBlock, headBlock uint64) error {
	for _, b := range makeBatches(idx.config.BatchSize, fromBlock, toBlock) {
		start := time.Now()
		logs, cacheHit, retryCount, err := idx.fetchLogBatch(ctx, b.from, b.to, headBlock)
		if err != nil {
			return err
		}

		if len(logs) > 0 {
			if err := processLogs(ctx, idx.handler, logs); err != nil {
				return err
			}
		}
		if err := idx.checkpoint(ctx, b.to); err != nil {
			return err
		}

		emitBatch(idx.config.Hooks, BatchStats{
			FromBlock:  b.from,
			ToBlock:    b.to,
			LogCount:   len(logs),
			Duration:   time.Since(start),
			CacheHit:   cacheHit,
			RetryCount: retryCount,
		})
	}
	return nil
}

func (idx *Indexer) fetchLogBatch(ctx context.Context, fromBlock, toBlock, headBlock uint64) ([]types.Log, bool, int, error) {
	if idx.config.LogCache != nil {
		logs, ok, err := idx.config.LogCache.Load(ctx, fromBlock, toBlock)
		if err != nil {
			return nil, false, 0, fmt.Errorf("indexer: load log cache %d-%d: %w", fromBlock, toBlock, err)
		}
		if ok {
			return logs, true, 0, nil
		}
	}

	query := idx.handler.Filter()
	query.FromBlock = new(big.Int).SetUint64(fromBlock)
	query.ToBlock = new(big.Int).SetUint64(toBlock)

	var fetched []types.Log
	retryCount, err := withRetry(ctx, idx.config.Retry, idx.config.Logger, func() error {
		var err error
		fetched, err = idx.client.FilterLogs(ctx, query)
		return err
	})
	if err != nil {
		return nil, false, retryCount, fmt.Errorf("indexer: filter logs from %d to %d: %w", fromBlock, toBlock, err)
	}

	if idx.config.LogCache != nil && headBlock > toBlock && headBlock-toBlock > idx.config.ReorgDepth {
		if err := idx.config.LogCache.Save(ctx, fromBlock, toBlock, fetched); err != nil {
			return nil, false, retryCount, fmt.Errorf("indexer: save log cache %d-%d: %w", fromBlock, toBlock, err)
		}
	}

	return fetched, false, retryCount, nil
}

func processLogs(ctx context.Context, handler Handler, logs []types.Log) error {
	slices.SortFunc(logs, func(a, b types.Log) int {
		if a.BlockNumber != b.BlockNumber {
			return cmp.Compare(a.BlockNumber, b.BlockNumber)
		}
		return cmp.Compare(a.Index, b.Index)
	})

	for len(logs) > 0 {
		blockNumber := logs[0].BlockNumber
		end := 1
		for end < len(logs) && logs[end].BlockNumber == blockNumber {
			end++
		}

		if err := handler.HandleLogs(ctx, logs[:end]); err != nil {
			return err
		}
		logs = logs[end:]
	}

	return nil
}

type batch struct {
	from uint64
	to   uint64
}

func makeBatches(size, from, to uint64) []batch {
	batches := make([]batch, 0, ((to-from)/size)+1)
	for from <= to {
		batchTo := from + size - 1
		if batchTo < from || batchTo > to {
			batchTo = to
		}
		batches = append(batches, batch{from: from, to: batchTo})
		from = batchTo + 1
	}
	return batches
}
