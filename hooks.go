package indexer

import (
	"context"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
)

func emitBatch(hooks Hooks, stats BatchStats) {
	if hooks.OnBatch != nil {
		hooks.OnBatch(stats)
	}
}

func emitCheckpoint(hooks Hooks, stats CheckpointStats) {
	if hooks.OnCheckpoint != nil {
		hooks.OnCheckpoint(stats)
	}
}

func emitReorg(ctx context.Context, logger *slog.Logger, hooks Hooks, stats ReorgStats) {
	attrs := []slog.Attr{
		slog.Uint64("block", stats.Block),
		slog.Uint64("parent_block", stats.ParentBlock),
		slog.String("parent_hash", stats.ParentHash.Hex()),
		slog.String("expected_hash", stats.ExpectedHash.Hex()),
	}
	if stats.CanonicalHash != (common.Hash{}) {
		attrs = append(attrs, slog.String("canonical_hash", stats.CanonicalHash.Hex()))
	}
	logger.LogAttrs(ctx, slog.LevelWarn, "chain reorg detected", attrs...)

	if hooks.OnReorg != nil {
		hooks.OnReorg(stats)
	}
}
