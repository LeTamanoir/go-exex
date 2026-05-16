package indexer

import (
	"errors"
	"reflect"

	"github.com/ethereum/go-ethereum/core/types"
)

func validateHeader(head *types.Header) error {
	if head == nil {
		return errors.New("indexer: head must not be nil")
	}
	if head.Number == nil {
		return errors.New("indexer: head number must not be nil")
	}
	return nil
}

func validateConfig(cfg Config) error {
	if cfg.BatchSize == 0 {
		return errors.New("indexer: batch size must be greater than zero")
	}
	if cfg.ReorgDepth == 0 {
		return errors.New("indexer: reorg depth must be greater than zero")
	}
	if cfg.Checkpoints != nil && cfg.CheckpointInterval == 0 {
		return errors.New("indexer: checkpoint interval must be greater than zero when checkpointing is enabled")
	}
	if cfg.Retry.InitialBackoff <= 0 {
		return errors.New("indexer: retry initial backoff must be greater than zero")
	}
	if cfg.Retry.MaxBackoff <= 0 {
		return errors.New("indexer: retry max backoff must be greater than zero")
	}
	if cfg.Retry.MaxBackoff < cfg.Retry.InitialBackoff {
		return errors.New("indexer: retry max backoff must be greater than or equal to initial backoff")
	}
	if cfg.Retry.Retryable == nil {
		return errors.New("indexer: retry policy must include a retryable function")
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func copyHeader(header *types.Header) *types.Header {
	if header == nil {
		return nil
	}
	return types.CopyHeader(header)
}
