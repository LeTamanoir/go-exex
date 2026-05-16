package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	defaultBatchSize  = uint64(2_000)
	defaultReorgDepth = uint64(128)
)

var (
	// ErrReorgRequiresReset is returned when a reorg invalidates the current
	// indexer state, no checkpoint can be loaded, and the handler cannot reset.
	ErrReorgRequiresReset = errors.New("indexer: automatic reorg recovery requires checkpointing or a resettable handler")
)

// Client is the Ethereum RPC client surface used by Indexer.
type Client interface {
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
}

// Handler owns the caller's indexed state.
//
// Filter returns the base Ethereum log filter. Sync methods set FromBlock and
// ToBlock for each requested batch while preserving addresses, topics, and
// other filter fields.
//
// HandleLogs receives logs sorted by transaction/log index and grouped by
// block. A single call never contains logs from multiple blocks.
type Handler interface {
	Filter() ethereum.FilterQuery
	HandleLogs(ctx context.Context, logs []types.Log) error
}

// Resetter can be implemented by handlers that know how to clear their state.
// It is used for automatic reorg recovery when checkpointing cannot restore a
// clean state.
type Resetter interface {
	Reset()
}

// Indexer coordinates log fetching, optional cache/checkpoint storage, and
// delivery to a Handler.
type Indexer struct {
	client  Client
	handler Handler
	config  Config

	head           *types.Header
	lastCheckpoint uint64
	blockHashes    map[uint64]common.Hash
}

// New creates an Indexer.
func New(client Client, handler Handler, cfg Config) (*Indexer, error) {
	if isNil(client) {
		return nil, errors.New("indexer: client must not be nil")
	}
	if isNil(handler) {
		return nil, errors.New("indexer: handler must not be nil")
	}
	cfg = cfg.withDefaults()
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	return &Indexer{
		client:      client,
		handler:     handler,
		config:      cfg,
		blockHashes: make(map[uint64]common.Hash, cfg.ReorgDepth),
	}, nil
}

// Handler returns the handler currently owned by the indexer. If a checkpoint
// is loaded, this may be a restored handler value.
func (idx *Indexer) Handler() Handler {
	return idx.handler
}

// Head returns a copy of the latest header successfully indexed by this
// Indexer.
func (idx *Indexer) Head() *types.Header {
	return copyHeader(idx.head)
}

// Sync is a compatibility alias for SyncTo.
func (idx *Indexer) Sync(ctx context.Context, head *types.Header) error {
	return idx.SyncTo(ctx, head)
}

// SyncTo indexes logs up to and including head.
func (idx *Indexer) SyncTo(ctx context.Context, head *types.Header) error {
	if err := validateHeader(head); err != nil {
		return err
	}

	target := head.Number.Uint64()
	if recovery, ok, err := detectReorg(ctx, idx.client, idx.head, head, idx.blockHashes); err != nil {
		return err
	} else if ok {
		if err := idx.recoverFromReorg(ctx, recovery); err != nil {
			return err
		}
	}

	if idx.head == nil {
		if _, err := idx.loadCheckpoint(ctx, target); err != nil {
			return err
		}
	}

	from := idx.fromBlock()
	if target < from {
		return nil
	}

	if err := idx.fetchAndProcessLogs(ctx, from, target, target); err != nil {
		idx.invalidateHead()
		return err
	}

	idx.recordHead(head)
	idx.cleanupBlockHashes(head)
	return nil
}

// SyncRange indexes logs from from to to, inclusive, and records the header for
// to after the range succeeds.
func (idx *Indexer) SyncRange(ctx context.Context, from, to uint64) error {
	if to < from {
		return nil
	}

	head, err := idx.client.HeaderByNumber(ctx, new(big.Int).SetUint64(to))
	if err != nil {
		return fmt.Errorf("indexer: fetch range head %d: %w", to, err)
	}

	if err := idx.fetchAndProcessLogs(ctx, from, to, to); err != nil {
		idx.invalidateHead()
		return err
	}

	idx.recordHead(head)
	idx.cleanupBlockHashes(head)
	return nil
}

// SyncWithRetry calls SyncTo and retries failures according to the configured
// RetryPolicy. A non-positive MaxAttempts means retry until the context ends.
func (idx *Indexer) SyncWithRetry(ctx context.Context, head *types.Header) error {
	_, err := withRetry(ctx, idx.config.Retry, idx.config.Logger, func() error {
		return idx.SyncTo(ctx, head)
	})
	return err
}

func withRetry(ctx context.Context, policy RetryPolicy, logger *slog.Logger, fn func() error) (int, error) {
	backoff := policy.InitialBackoff
	attempt := 0
	retries := 0

	for {
		attempt++
		err := fn()
		if err == nil {
			return retries, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return retries, ctxErr
		}
		if policy.MaxAttempts > 0 && attempt >= policy.MaxAttempts {
			return retries, err
		}
		if !policy.Retryable(err) {
			return retries, err
		}

		logger.Warn("retrying indexer operation",
			slog.Duration("backoff", backoff),
			slog.Int("attempt", attempt),
			slog.Any("error", err))

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return retries, ctx.Err()
		}

		retries++
		backoff *= 2
		if backoff > policy.MaxBackoff {
			backoff = policy.MaxBackoff
		}
	}
}
