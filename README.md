# indexer

`indexer` is a small Go package for indexing Ethereum logs.

```go
idx, err := indexer.New(client, handler, indexer.Config{
	StartBlock:         18_000_000,
	BatchSize:          2_000,
	CheckpointInterval: 10_000,
	Checkpoints:        indexer.FileCheckpoints(".cache/aave"),
	LogCache:           indexer.FileLogCache(".cache/aave"),
	Logger:             logger,
})
if err != nil {
	return err
}

if err := idx.SyncTo(ctx, head); err != nil {
	return err
}
```

Handlers provide a full `ethereum.FilterQuery` and receive sorted logs grouped
by block:

```go
type Handler interface {
	Filter() ethereum.FilterQuery
	HandleLogs(ctx context.Context, logs []types.Log) error
}
```

Checkpoint and log-cache storage are optional. The default writes nothing to
disk. Automatic reorg recovery requires checkpointing or a handler that
implements:

```go
type Resetter interface {
	Reset()
}
```
