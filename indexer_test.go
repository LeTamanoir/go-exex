package indexer

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type fakeClient struct {
	headers map[uint64]*types.Header
	logs    map[batch][]types.Log
}

func (c *fakeClient) FilterLogs(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	key := batch{from: query.FromBlock.Uint64(), to: query.ToBlock.Uint64()}
	logs := c.logs[key]
	cp := make([]types.Log, len(logs))
	copy(cp, logs)
	return cp, nil
}

func (c *fakeClient) HeaderByNumber(_ context.Context, number *big.Int) (*types.Header, error) {
	if number == nil {
		return nil, errors.New("latest header is not configured")
	}
	header := c.headers[number.Uint64()]
	if header == nil {
		return makeHeader(number.Uint64()), nil
	}
	return copyHeader(header), nil
}

type testHandler struct {
	Count      int
	filter     ethereum.FilterQuery
	handled    [][]types.Log
	handleErr  error
	resetCount int
}

func (h *testHandler) Filter() ethereum.FilterQuery {
	return h.filter
}

func (h *testHandler) HandleLogs(_ context.Context, logs []types.Log) error {
	if h.handleErr != nil {
		return h.handleErr
	}
	h.Count += len(logs)
	cp := make([]types.Log, len(logs))
	copy(cp, logs)
	h.handled = append(h.handled, cp)
	return nil
}

func (h *testHandler) Reset() {
	h.Count = 0
	h.handled = nil
	h.resetCount++
}

type nonResetHandler struct {
	filter ethereum.FilterQuery
}

func (h *nonResetHandler) Filter() ethereum.FilterQuery {
	return h.filter
}

func (h *nonResetHandler) HandleLogs(context.Context, []types.Log) error {
	return nil
}

func makeHeader(number uint64) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(number)}
}

func makeHeaderWithExtra(number uint64, extra string) *types.Header {
	return &types.Header{
		Number: new(big.Int).SetUint64(number),
		Extra:  []byte(extra),
	}
}

func makeHeaderWithParent(number uint64, parent common.Hash) *types.Header {
	return &types.Header{
		Number:     new(big.Int).SetUint64(number),
		ParentHash: parent,
	}
}

func newTestIndexer(t *testing.T, handler *testHandler, configs ...Config) *Indexer {
	t.Helper()
	var cfg Config
	if len(configs) > 0 {
		cfg = configs[0]
	}
	idx, err := New(&fakeClient{}, handler, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestNewValidation(t *testing.T) {
	handler := &testHandler{}

	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{
			name: "nil client",
			run: func() error {
				_, err := New(nil, handler, Config{})
				return err
			},
			want: "client must not be nil",
		},
		{
			name: "typed nil handler",
			run: func() error {
				var nilHandler *testHandler
				_, err := New(&fakeClient{}, nilHandler, Config{})
				return err
			},
			want: "handler must not be nil",
		},
		{
			name: "checkpoint interval required",
			run: func() error {
				_, err := New(&fakeClient{}, handler, Config{Checkpoints: FileCheckpoints(t.TempDir())})
				return err
			},
			want: "checkpoint interval",
		},
		{
			name: "invalid retry policy",
			run: func() error {
				_, err := New(&fakeClient{}, handler, Config{Retry: RetryPolicy{InitialBackoff: -1}})
				return err
			},
			want: "retry initial backoff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestNilCheckpointsDisableCheckpointing(t *testing.T) {
	_, err := New(&fakeClient{}, &testHandler{}, Config{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMakeBatches(t *testing.T) {
	got := makeBatches(10, 100, 125)
	want := []batch{{from: 100, to: 109}, {from: 110, to: 119}, {from: 120, to: 125}}

	if len(got) != len(want) {
		t.Fatalf("got %d batches, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("batch %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestProcessLogBatchSortsAndGroupsByBlock(t *testing.T) {
	handler := &testHandler{}

	err := processLogs(context.Background(), handler, []types.Log{
		{BlockNumber: 3, Index: 1},
		{BlockNumber: 1, Index: 2},
		{BlockNumber: 1, Index: 0},
		{BlockNumber: 3, Index: 0},
		{BlockNumber: 2, Index: 0},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(handler.handled) != 3 {
		t.Fatalf("got %d HandleLogs calls, want 3", len(handler.handled))
	}
	if handler.handled[0][0].BlockNumber != 1 || handler.handled[0][0].Index != 0 || handler.handled[0][1].Index != 2 {
		t.Fatalf("first block not sorted/grouped correctly: %+v", handler.handled[0])
	}
	if handler.handled[1][0].BlockNumber != 2 {
		t.Fatalf("second call should be block 2: %+v", handler.handled[1])
	}
	if handler.handled[2][0].BlockNumber != 3 || handler.handled[2][0].Index != 0 || handler.handled[2][1].Index != 1 {
		t.Fatalf("third block not sorted/grouped correctly: %+v", handler.handled[2])
	}
}

func TestProcessLogBatchReturnsHandlerError(t *testing.T) {
	handler := &testHandler{handleErr: errors.New("boom")}

	err := processLogs(context.Background(), handler, []types.Log{{BlockNumber: 1}})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("got %v, want boom", err)
	}
}

func TestHeadReturnsCopy(t *testing.T) {
	handler := &testHandler{}
	idx := newTestIndexer(t, handler)
	header := makeHeaderWithExtra(100, "original")

	idx.recordHead(header)
	header.Number.SetUint64(200)
	header.Extra[0] = 'x'

	got := idx.Head()
	if got.Number.Uint64() != 100 || string(got.Extra) != "original" {
		t.Fatalf("stored head was mutated: number=%d extra=%q", got.Number.Uint64(), string(got.Extra))
	}

	got.Number.SetUint64(300)
	got.Extra[0] = 'y'
	got = idx.Head()
	if got.Number.Uint64() != 100 || string(got.Extra) != "original" {
		t.Fatalf("returned head mutation affected indexer: number=%d extra=%q", got.Number.Uint64(), string(got.Extra))
	}
}

func TestFileCheckpointsLoadClosestAndPrune(t *testing.T) {
	ctx := context.Background()
	store := FileCheckpoints(t.TempDir())

	for _, tc := range []struct {
		block uint64
		count int
	}{
		{block: 100, count: 1},
		{block: 200, count: 2},
		{block: 300, count: 3},
	} {
		data, err := encodeCheckpoint(&testHandler{Count: tc.count})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Save(ctx, tc.block, data); err != nil {
			t.Fatal(err)
		}
	}

	block, data, ok, err := store.Load(ctx, 250)
	if err != nil {
		t.Fatal(err)
	}
	loadedHandler, err := decodeCheckpoint(data, &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	loaded := loadedHandler.(*testHandler)
	if !ok || block != 200 || loaded.Count != 2 {
		t.Fatalf("got block=%d ok=%v count=%d, want block=200 ok=true count=2", block, ok, loaded.Count)
	}

	if err := store.Prune(ctx, 200); err != nil {
		t.Fatal(err)
	}

	block, data, ok, err = store.Load(ctx, 350)
	if err != nil {
		t.Fatal(err)
	}
	loadedHandler, err = decodeCheckpoint(data, &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	loaded = loadedHandler.(*testHandler)
	if !ok || block != 100 || loaded.Count != 1 {
		t.Fatalf("after prune got block=%d ok=%v count=%d, want block=100 ok=true count=1", block, ok, loaded.Count)
	}
}

func TestFileLogCacheRoundTrip(t *testing.T) {
	ctx := context.Background()
	cache := FileLogCache(t.TempDir())
	logs := []types.Log{{BlockNumber: 100, Index: 2}}

	got, ok, err := cache.Load(ctx, 100, 109)
	if err != nil {
		t.Fatal(err)
	}
	if ok || got != nil {
		t.Fatalf("empty cache got ok=%v logs=%v", ok, got)
	}

	if err := cache.Save(ctx, 100, 109, logs); err != nil {
		t.Fatal(err)
	}

	got, ok, err = cache.Load(ctx, 100, 109)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(got) != 1 || got[0].BlockNumber != 100 || got[0].Index != 2 {
		t.Fatalf("unexpected cache load: ok=%v logs=%+v", ok, got)
	}
}

func TestCheckpointIntervalAndHook(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var checkpoints []CheckpointStats
	handler := &testHandler{Count: 7}
	idx := newTestIndexer(t, handler, Config{
		Checkpoints:        FileCheckpoints(dir),
		CheckpointInterval: 100,
		Hooks: Hooks{
			OnCheckpoint: func(stats CheckpointStats) {
				checkpoints = append(checkpoints, stats)
			},
		},
	})

	if err := idx.checkpoint(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if err := idx.checkpoint(ctx, 50); err != nil {
		t.Fatal(err)
	}
	if err := idx.checkpoint(ctx, 110); err != nil {
		t.Fatal(err)
	}

	if len(checkpoints) != 2 {
		t.Fatalf("got %d checkpoint hooks, want 2", len(checkpoints))
	}

	block, data, ok, err := FileCheckpoints(dir).Load(ctx, 110)
	if err != nil {
		t.Fatal(err)
	}
	loadedHandler, err := decodeCheckpoint(data, &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	loaded := loadedHandler.(*testHandler)
	if !ok || block != 110 || loaded.Count != 7 {
		t.Fatalf("got block=%d ok=%v count=%d, want block=110 ok=true count=7", block, ok, loaded.Count)
	}
}

func TestFetchAndProcessLogsFromCacheEmitsBatchStats(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	cache := FileLogCache(cacheDir)
	if err := cache.Save(ctx, 100, 109, []types.Log{
		{BlockNumber: 101, Index: 1},
		{BlockNumber: 101, Index: 0},
	}); err != nil {
		t.Fatal(err)
	}

	var batches []BatchStats
	handler := &testHandler{}
	idx := newTestIndexer(t, handler, Config{
		BatchSize: 10,
		LogCache:  cache,
		Hooks: Hooks{
			OnBatch: func(stats BatchStats) {
				batches = append(batches, stats)
			},
		},
	})

	if err := idx.fetchAndProcessLogs(ctx, 100, 109, 1_000); err != nil {
		t.Fatal(err)
	}

	if len(handler.handled) != 1 || len(handler.handled[0]) != 2 {
		t.Fatalf("unexpected handled logs: %+v", handler.handled)
	}
	if len(batches) != 1 || !batches[0].CacheHit || batches[0].LogCount != 2 {
		t.Fatalf("unexpected batch stats: %+v", batches)
	}
}

func TestFetchAndProcessLogsCheckpointsEmptyBatch(t *testing.T) {
	ctx := context.Background()
	checkpointDir := t.TempDir()
	cache := FileLogCache(t.TempDir())
	if err := cache.Save(ctx, 100, 109, nil); err != nil {
		t.Fatal(err)
	}

	handler := &testHandler{Count: 7}
	idx := newTestIndexer(t, handler, Config{
		BatchSize:          10,
		LogCache:           cache,
		Checkpoints:        FileCheckpoints(checkpointDir),
		CheckpointInterval: 1,
	})

	if err := idx.fetchAndProcessLogs(ctx, 100, 109, 1_000); err != nil {
		t.Fatal(err)
	}

	block, data, ok, err := FileCheckpoints(checkpointDir).Load(ctx, 109)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || block != 109 {
		t.Fatalf("got block=%d ok=%v, want block=109 ok=true", block, ok)
	}
	loadedHandler, err := decodeCheckpoint(data, &testHandler{})
	if err != nil {
		t.Fatal(err)
	}
	loaded := loadedHandler.(*testHandler)
	if loaded.Count != 7 {
		t.Fatalf("checkpoint count=%d, want 7", loaded.Count)
	}
}

func TestReorgWithoutCheckpointRequiresResetter(t *testing.T) {
	handler := &nonResetHandler{}
	idx, err := New(&fakeClient{}, handler, Config{StartBlock: 100})
	if err != nil {
		t.Fatal(err)
	}

	oldHead := makeHeader(10)
	idx.recordHead(oldHead)

	err = idx.SyncTo(context.Background(), makeHeaderWithParent(11, common.HexToHash("0xbad")))
	if !errors.Is(err, ErrReorgRequiresReset) {
		t.Fatalf("got %v, want ErrReorgRequiresReset", err)
	}
}

func TestReorgWithResetterResetsAndReplaysFromStart(t *testing.T) {
	handler := &testHandler{Count: 42}
	idx := newTestIndexer(t, handler, Config{StartBlock: 100})
	oldHead := makeHeader(10)
	idx.recordHead(oldHead)

	err := idx.SyncTo(context.Background(), makeHeaderWithParent(11, common.HexToHash("0xbad")))
	if err != nil {
		t.Fatal(err)
	}
	if handler.resetCount != 1 || handler.Count != 0 {
		t.Fatalf("resetCount=%d count=%d, want resetCount=1 count=0", handler.resetCount, handler.Count)
	}
	if idx.Head() != nil {
		t.Fatal("head should stay nil when target is before start block after reorg")
	}
}

func TestSyncToDetectsCanonicalReorg(t *testing.T) {
	oldHead := makeHeaderWithExtra(100, "old")
	canonicalHead := makeHeaderWithExtra(100, "new")
	client := &fakeClient{
		headers: map[uint64]*types.Header{
			100: canonicalHead,
		},
	}
	handler := &testHandler{Count: 42}
	idx, err := New(client, handler, Config{StartBlock: 1_000})
	if err != nil {
		t.Fatal(err)
	}
	idx.recordHead(oldHead)

	if err := idx.SyncTo(context.Background(), makeHeaderWithExtra(110, "target")); err != nil {
		t.Fatal(err)
	}

	if handler.resetCount != 1 || handler.Count != 0 {
		t.Fatalf("resetCount=%d count=%d, want resetCount=1 count=0", handler.resetCount, handler.Count)
	}
	if idx.Head() != nil {
		t.Fatal("head should stay nil when target is before start block after canonical reorg")
	}
}
