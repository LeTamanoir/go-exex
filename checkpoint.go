package indexer

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/big"
	"reflect"
	"time"
)

func (idx *Indexer) checkpoint(ctx context.Context, blockNumber uint64) error {
	if idx.config.Checkpoints == nil {
		return nil
	}
	if idx.lastCheckpoint != 0 && blockNumber-idx.lastCheckpoint < idx.config.CheckpointInterval {
		return nil
	}

	start := time.Now()
	data, err := encodeCheckpoint(idx.handler)
	if err != nil {
		return fmt.Errorf("indexer: encode checkpoint at %d: %w", blockNumber, err)
	}
	if err := idx.config.Checkpoints.Save(ctx, blockNumber, data); err != nil {
		return fmt.Errorf("indexer: save checkpoint at %d: %w", blockNumber, err)
	}
	idx.lastCheckpoint = blockNumber
	emitCheckpoint(idx.config.Hooks, CheckpointStats{
		Block:    blockNumber,
		Duration: time.Since(start),
		Store:    describeStore(idx.config.Checkpoints),
	})
	return nil
}

func (idx *Indexer) loadCheckpoint(ctx context.Context, target uint64) (bool, error) {
	if idx.config.Checkpoints == nil {
		return false, nil
	}

	start := time.Now()
	block, data, ok, err := idx.config.Checkpoints.Load(ctx, target)
	if err != nil {
		return false, fmt.Errorf("indexer: load checkpoint at or before %d: %w", target, err)
	}
	if !ok {
		return false, nil
	}

	handler, err := decodeCheckpoint(data, idx.handler)
	if err != nil {
		return false, fmt.Errorf("indexer: decode checkpoint at %d: %w", block, err)
	}

	head, err := idx.client.HeaderByNumber(ctx, new(big.Int).SetUint64(block))
	if err != nil {
		return false, fmt.Errorf("indexer: fetch checkpoint header %d: %w", block, err)
	}

	idx.handler = handler
	idx.head = copyHeader(head)
	idx.blockHashes[block] = head.Hash()
	idx.lastCheckpoint = block
	emitCheckpoint(idx.config.Hooks, CheckpointStats{
		Block:    block,
		Duration: time.Since(start),
		Store:    describeStore(idx.config.Checkpoints),
		Loaded:   true,
	})
	return true, nil
}

func encodeCheckpoint(handler Handler) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(handler); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCheckpoint(data []byte, current Handler) (Handler, error) {
	typ := reflect.TypeOf(current)
	if typ == nil {
		return nil, fmt.Errorf("checkpoint handler type is nil")
	}

	if typ.Kind() == reflect.Pointer {
		value := reflect.New(typ.Elem())
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(value.Interface()); err != nil {
			return nil, err
		}
		handler, ok := value.Interface().(Handler)
		if !ok {
			return nil, fmt.Errorf("checkpoint decoded %T, want Handler", value.Interface())
		}
		return handler, nil
	}

	value := reflect.New(typ)
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(value.Interface()); err != nil {
		return nil, err
	}
	handler, ok := value.Elem().Interface().(Handler)
	if !ok {
		return nil, fmt.Errorf("checkpoint decoded %T, want Handler", value.Elem().Interface())
	}
	return handler, nil
}
