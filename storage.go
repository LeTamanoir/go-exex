package indexer

import (
	"cmp"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
)

// CheckpointStore persists encoded handler state at selected blocks.
type CheckpointStore interface {
	Save(ctx context.Context, block uint64, data []byte) error
	Load(ctx context.Context, target uint64) (block uint64, data []byte, ok bool, err error)
	Prune(ctx context.Context, from uint64) error
}

// FileCheckpoints stores checkpoints as gob files in path.
func FileCheckpoints(path string) CheckpointStore {
	return fileCheckpointStore{path: path}
}

type fileCheckpointStore struct {
	path string
}

func (s fileCheckpointStore) Save(_ context.Context, block uint64, data []byte) error {
	if err := os.MkdirAll(s.path, 0755); err != nil {
		return err
	}

	return os.WriteFile(s.checkpointFile(block), data, 0644)
}

func (s fileCheckpointStore) Load(_ context.Context, target uint64) (uint64, []byte, bool, error) {
	blocks, err := s.checkpointBlocks()
	if err != nil {
		return 0, nil, false, err
	}
	if len(blocks) == 0 {
		return 0, nil, false, nil
	}

	idx, ok := slices.BinarySearchFunc(blocks, target, func(block uint64, target uint64) int {
		return cmp.Compare(block, target)
	})
	if !ok {
		idx--
	}
	if idx < 0 {
		return 0, nil, false, nil
	}

	block := blocks[idx]
	data, err := os.ReadFile(s.checkpointFile(block))
	if err != nil {
		return 0, nil, false, err
	}

	return block, data, true, nil
}

func (s fileCheckpointStore) Prune(_ context.Context, from uint64) error {
	blocks, err := s.checkpointBlocks()
	if err != nil {
		return err
	}
	for _, block := range blocks {
		if block < from {
			continue
		}
		if err := os.Remove(s.checkpointFile(block)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (s fileCheckpointStore) String() string {
	return s.path
}

func (s fileCheckpointStore) checkpointFile(block uint64) string {
	return filepath.Join(s.path, fmt.Sprintf("checkpoint-%d.gob", block))
}

func (s fileCheckpointStore) checkpointBlocks() ([]uint64, error) {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	blocks := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "checkpoint-") || !strings.HasSuffix(name, ".gob") {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(name, "checkpoint-"), ".gob")
		block, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			continue
		}
		blocks = append(blocks, block)
	}
	slices.Sort(blocks)
	return blocks, nil
}

// LogCache persists fetched logs for immutable-enough historical ranges.
type LogCache interface {
	Load(ctx context.Context, from, to uint64) ([]types.Log, bool, error)
	Save(ctx context.Context, from, to uint64, logs []types.Log) error
}

// FileLogCache stores log batches as gob files in path.
func FileLogCache(path string) LogCache {
	return fileLogCache{path: path}
}

type fileLogCache struct {
	path string
}

func (c fileLogCache) Load(_ context.Context, from, to uint64) ([]types.Log, bool, error) {
	f, err := os.Open(c.logFile(from, to))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer f.Close()

	var logs []types.Log
	if err := gob.NewDecoder(f).Decode(&logs); err != nil {
		return nil, false, err
	}
	return logs, true, nil
}

func (c fileLogCache) Save(_ context.Context, from, to uint64, logs []types.Log) error {
	if err := os.MkdirAll(c.path, 0755); err != nil {
		return err
	}

	f, err := os.Create(c.logFile(from, to))
	if err != nil {
		return err
	}
	defer f.Close()

	return gob.NewEncoder(f).Encode(logs)
}

func (c fileLogCache) String() string {
	return c.path
}

func (c fileLogCache) logFile(from, to uint64) string {
	return filepath.Join(c.path, fmt.Sprintf("logs-%d-%d.gob", from, to))
}

func describeStore(store any) string {
	if store == nil {
		return ""
	}
	if s, ok := store.(fmt.Stringer); ok {
		return s.String()
	}
	return strings.TrimPrefix(fmt.Sprintf("%T", store), "*")
}
