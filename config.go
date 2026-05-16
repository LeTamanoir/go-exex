package indexer

import (
	"io"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Config contains Indexer settings. The zero value is valid and writes no
// checkpoints or log cache entries.
type Config struct {
	StartBlock         uint64
	BatchSize          uint64
	CheckpointInterval uint64
	ReorgDepth         uint64
	Checkpoints        CheckpointStore
	LogCache           LogCache
	Logger             *slog.Logger
	Retry              RetryPolicy
	Hooks              Hooks
}

func (cfg Config) withDefaults() Config {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.ReorgDepth == 0 {
		cfg.ReorgDepth = defaultReorgDepth
	}
	if cfg.Logger == nil {
		cfg.Logger = silentLogger()
	}
	if cfg.Retry.InitialBackoff == 0 && cfg.Retry.MaxBackoff == 0 && cfg.Retry.MaxAttempts == 0 && cfg.Retry.Retryable == nil {
		cfg.Retry = DefaultRetryPolicy()
	}
	return cfg
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Hooks contains optional observability callbacks.
type Hooks struct {
	OnBatch      func(BatchStats)
	OnCheckpoint func(CheckpointStats)
	OnReorg      func(ReorgStats)
}

// BatchStats describes one fetched and processed batch.
type BatchStats struct {
	FromBlock  uint64
	ToBlock    uint64
	LogCount   int
	Duration   time.Duration
	CacheHit   bool
	RetryCount int
}

// CheckpointStats describes a checkpoint save or load.
type CheckpointStats struct {
	Block    uint64
	Duration time.Duration
	Store    string
	Loaded   bool
}

// ReorgStats describes a detected reorg and the recovery path taken.
type ReorgStats struct {
	Block            uint64
	ParentBlock      uint64
	ParentHash       common.Hash
	ExpectedHash     common.Hash
	CanonicalHash    common.Hash
	CheckpointLoaded bool
	CheckpointBlock  uint64
	Reset            bool
}
