package indexer

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type reorgRecovery struct {
	pruneFrom uint64
	stats     ReorgStats
}

func (idx *Indexer) recoverFromReorg(ctx context.Context, recovery reorgRecovery) error {
	stats := recovery.stats
	idx.invalidateHead()
	if idx.config.Checkpoints != nil {
		if err := idx.config.Checkpoints.Prune(ctx, recovery.pruneFrom); err != nil {
			return fmt.Errorf("indexer: prune checkpoints after reorg: %w", err)
		}
	}

	if loaded, err := idx.loadCheckpoint(ctx, recovery.pruneFrom); err != nil {
		return err
	} else if loaded {
		stats.CheckpointLoaded = true
		if idx.head != nil {
			stats.CheckpointBlock = idx.head.Number.Uint64()
		}
		emitReorg(ctx, idx.config.Logger, idx.config.Hooks, stats)
		return nil
	}

	resetter, ok := any(idx.handler).(Resetter)
	if !ok {
		emitReorg(ctx, idx.config.Logger, idx.config.Hooks, stats)
		return ErrReorgRequiresReset
	}
	resetter.Reset()
	stats.Reset = true
	emitReorg(ctx, idx.config.Logger, idx.config.Hooks, stats)
	return nil
}

func detectReorg(ctx context.Context, client Client, currentHead, head *types.Header, blockHashes map[uint64]common.Hash) (reorgRecovery, bool, error) {
	if head.Number.Sign() == 0 {
		return reorgRecovery{}, false, nil
	}

	parentBlockNum := head.Number.Uint64() - 1
	if expectedHash, exists := blockHashes[parentBlockNum]; exists && head.ParentHash != expectedHash {
		return reorgRecovery{
			pruneFrom: parentBlockNum,
			stats: ReorgStats{
				Block:        head.Number.Uint64(),
				ParentBlock:  parentBlockNum,
				ParentHash:   head.ParentHash,
				ExpectedHash: expectedHash,
			},
		}, true, nil
	}

	if currentHead == nil {
		return reorgRecovery{}, false, nil
	}

	canonical, err := client.HeaderByNumber(ctx, currentHead.Number)
	if err != nil {
		return reorgRecovery{}, false, fmt.Errorf("indexer: fetch canonical header %s: %w", currentHead.Number, err)
	}
	if canonical.Hash() == currentHead.Hash() {
		return reorgRecovery{}, false, nil
	}

	indexedBlock := currentHead.Number.Uint64()
	parentBlock := indexedBlock
	if indexedBlock > 0 {
		parentBlock = indexedBlock - 1
	}

	return reorgRecovery{
		pruneFrom: indexedBlock,
		stats: ReorgStats{
			Block:         indexedBlock,
			ParentBlock:   parentBlock,
			ParentHash:    canonical.ParentHash,
			ExpectedHash:  currentHead.Hash(),
			CanonicalHash: canonical.Hash(),
		},
	}, true, nil
}

func (idx *Indexer) invalidateHead() {
	idx.head = nil
	idx.lastCheckpoint = 0
	idx.blockHashes = make(map[uint64]common.Hash, idx.config.ReorgDepth)
}

func (idx *Indexer) cleanupBlockHashes(head *types.Header) {
	blockNumber := head.Number.Uint64()
	if blockNumber <= idx.config.ReorgDepth {
		return
	}

	minBlock := blockNumber - idx.config.ReorgDepth
	for block := range idx.blockHashes {
		if block < minBlock {
			delete(idx.blockHashes, block)
		}
	}
}

func (idx *Indexer) recordHead(head *types.Header) {
	idx.head = copyHeader(head)
	idx.blockHashes[head.Number.Uint64()] = head.Hash()
}

func (idx *Indexer) fromBlock() uint64 {
	if idx.head == nil {
		return idx.config.StartBlock
	}
	return idx.head.Number.Uint64() + 1
}
