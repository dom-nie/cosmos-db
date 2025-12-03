package db

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/spf13/cast"
)

const (
	// PebbleDB tuning constants optimized for Cosmos SDK workloads
	// on dedicated machines.

	// pebbleMemTableSize is the size of each memtable (64 MB).
	// Matches sei-db configuration for Cosmos SDK workloads.
	pebbleMemTableSize = 64 << 20

	// pebbleMemTableStopWritesThreshold stops writes when this many memtables
	// are queued for flush. With 64 MB memtables, this means up to 256 MB.
	pebbleMemTableStopWritesThreshold = 4

	// pebbleL0CompactionThreshold triggers compaction when L0 has this many files.
	// Lower than default (4) for more aggressive compaction.
	pebbleL0CompactionThreshold = 2

	// pebbleL0StopWritesThreshold stops writes when L0 reaches this file count.
	// Provides headroom during write bursts without excessive read amplification.
	pebbleL0StopWritesThreshold = 500

	// pebbleLBaseMaxBytes is the maximum size of the base level (64 MB).
	pebbleLBaseMaxBytes = 64 << 20

	// pebbleBlockSize is the size of data blocks in sstables (32 KB).
	// Optimized for Cosmos SDK's mix of point lookups and range scans.
	pebbleBlockSize = 32 << 10

	// pebbleIndexBlockSize is the size of index blocks (256 KB).
	pebbleIndexBlockSize = 256 << 10

	// pebbleBloomFilterBits is bits per key for bloom filters.
	// 10 bits provides ~1% false positive rate.
	pebbleBloomFilterBits = 10

	// pebbleLevelCount is the number of LSM tree levels.
	pebbleLevelCount = 7

	// pebbleHotLevels defines how many levels use fast compression (Snappy).
	// Levels 0-2 are hot (Snappy), levels 3-6 are cold (Zstd).
	pebbleHotLevels = 3

	// pebbleCacheFallback is the fallback cache size when RAM detection fails (4 GB).
	pebbleCacheFallback = 4 << 30

	// pebbleCacheMax is the maximum cache size to prevent excessive GC pauses.
	// 16 GB provides good performance while keeping GC pauses under 400ms.
	pebbleCacheMax = 16 << 30
)

// ForceSync
/*
This is set at compile time. Could be 0 or 1, defaults is 0.
It will force using Sync for NoSync functions (Set, Delete, Write)

Used as a workaround for chain-upgrade issue: At the upgrade-block, the sdk will panic without flushing data to disk or
closing dbs properly.

Upgrade guide:
	1. After seeing `UPGRADE "xxxx" NEED at height....`, restart current version with `-X github.com/tendermint/tm-db.ForceSync=1`
	2. Restart new version as normal


Example: Upgrading sifchain from v0.14.0 to v0.15.0

# log:
panic: UPGRADE "0.15.0" NEEDED at height: 8170210: {"binaries":{"linux/amd64":"https://github.com/Sifchain/sifnode/releases/download/v0.15.0/sifnoded-v0.15.0-linux-amd64.zip?checksum=0c03b5846c5a13dcc0d9d3127e4f0cee0aeddcf2165177b2f2e0d60dbcf1a5ea"}}

# step1
git reset --hard
git checkout v0.14.0
go mod edit -replace github.com/tendermint/tm-db=github.com/baabeetaa/tm-db@pebble
go mod tidy
go install -ldflags "-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=pebbledb -X github.com/tendermint/tm-db.ForceSync=1" ./cmd/sifnoded

$HOME/go/bin/sifnoded start --db_backend=pebbledb


# step 2
git reset --hard
git checkout v0.15.0
go mod edit -replace github.com/tendermint/tm-db=github.com/baabeetaa/tm-db@pebble
go mod tidy
go install -ldflags "-w -s -X github.com/cosmos/cosmos-sdk/types.DBBackend=pebbledb" ./cmd/sifnoded

$HOME/go/bin/sifnoded start --db_backend=pebbledb

*/
var (
	ForceSync   = "0"
	isForceSync = false
)

func init() {
	dbCreator := func(name string, dir string, opts Options) (DB, error) {
		return NewPebbleDB(name, dir, opts)
	}
	registerDBCreator(PebbleDBBackend, dbCreator, false)

	if ForceSync == "1" {
		isForceSync = true
	}
}

// PebbleDB is a PebbleDB backend.
type PebbleDB struct {
	db *pebble.DB
}

var _ DB = (*PebbleDB)(nil)

func NewPebbleDB(name string, dir string, opts Options) (DB, error) {
	// Detect system resources for auto-tuning
	sysRes := GetSystemResources()

	// Calculate cache size: 1/3 of RAM, capped at 16 GB to limit GC pauses
	cacheSize := sysRes.TotalRAM / 3
	if cacheSize == 0 {
		cacheSize = pebbleCacheFallback
	}
	if cacheSize > pebbleCacheMax {
		cacheSize = pebbleCacheMax
	}
	cache := pebble.NewCache(int64(cacheSize))
	defer cache.Unref()

	// Use all CPUs for compaction (dedicated machine assumption)
	compactionWorkers := sysRes.NumCPU
	if compactionWorkers <= 0 {
		compactionWorkers = 4
	}

	do := &pebble.Options{
		// Suppress noisy pebble info logs
		Logger: &fatalLogger{},

		// Auto-tuned based on system resources
		Cache:                    cache,
		MaxConcurrentCompactions: func() int { return compactionWorkers },

		// Fixed optimizations for Cosmos SDK workloads
		MemTableSize:                pebbleMemTableSize,
		MemTableStopWritesThreshold: pebbleMemTableStopWritesThreshold,
		L0CompactionThreshold:       pebbleL0CompactionThreshold,
		L0StopWritesThreshold:       pebbleL0StopWritesThreshold,
		LBaseMaxBytes:               pebbleLBaseMaxBytes,

		// Use latest format for best performance
		FormatMajorVersion: pebble.FormatNewest,

		// Configure 7-level LSM tree
		Levels: make([]pebble.LevelOptions, pebbleLevelCount),
	}

	// Configure per-level options
	for i := 0; i < len(do.Levels); i++ {
		l := &do.Levels[i]

		// Block configuration optimized for Cosmos SDK
		l.BlockSize = pebbleBlockSize
		l.IndexBlockSize = pebbleIndexBlockSize

		// Tiered compression:
		// - L0-L2 (hot): Snappy for faster compression/decompression
		// - L3-L6 (cold): Zstd for better compression ratio
		if i < pebbleHotLevels {
			l.Compression = pebble.SnappyCompression
		} else {
			l.Compression = pebble.ZstdCompression
		}

		// Bloom filter on all levels except bottommost (level 6)
		// Bottommost level doesn't benefit as much from bloom filters
		if i < pebbleLevelCount-1 {
			l.FilterPolicy = bloom.FilterPolicy(pebbleBloomFilterBits)
			l.FilterType = pebble.TableFilter
		}

		// Target file size doubles per level
		if i == 0 {
			l.TargetFileSize = 2 << 20 // 2 MB for L0
		} else {
			l.TargetFileSize = do.Levels[i-1].TargetFileSize * 2
		}

		l.EnsureDefaults()
	}

	// Set flush split bytes to match L0 target file size
	do.FlushSplitBytes = do.Levels[0].TargetFileSize

	// Apply defaults for any unset options
	do.EnsureDefaults()

	// Override from user-provided options
	if opts != nil {
		if files := cast.ToInt(opts.Get("maxopenfiles")); files > 0 {
			do.MaxOpenFiles = files
		}
	}

	dbPath := filepath.Join(dir, name+DBFileSuffix)
	p, err := pebble.Open(dbPath, do)
	if err != nil {
		return nil, err
	}

	return &PebbleDB{db: p}, nil
}

// NewPebbleDBWithOpts creates a PebbleDB with full control over pebble.Options.
// Use this when you need custom configuration beyond what NewPebbleDB provides.
func NewPebbleDBWithOpts(name string, dir string, o *pebble.Options) (*PebbleDB, error) {
	dbPath := filepath.Join(dir, name+DBFileSuffix)
	p, err := pebble.Open(dbPath, o)
	if err != nil {
		return nil, err
	}
	return &PebbleDB{db: p}, nil
}

// Get implements DB.
func (db *PebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	res, closer, err := db.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	return cp(res), nil
}

// Has implements DB.
func (db *PebbleDB) Has(key []byte) (bool, error) {
	// fmt.Println("PebbleDB.Has")
	if len(key) == 0 {
		return false, errKeyEmpty
	}
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *PebbleDB) Set(key []byte, value []byte) error {
	// fmt.Println("PebbleDB.Set")
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	wopts := pebble.NoSync
	if isForceSync {
		wopts = pebble.Sync
	}

	err := db.db.Set(key, value, wopts)
	if err != nil {
		return err
	}
	return nil
}

// SetSync implements DB.
func (db *PebbleDB) SetSync(key []byte, value []byte) error {
	// fmt.Println("PebbleDB.SetSync")
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.db.Set(key, value, pebble.Sync)
	if err != nil {
		return err
	}
	return nil
}

// Delete implements DB.
func (db *PebbleDB) Delete(key []byte) error {
	// fmt.Println("PebbleDB.Delete")
	if len(key) == 0 {
		return errKeyEmpty
	}

	wopts := pebble.NoSync
	if isForceSync {
		wopts = pebble.Sync
	}
	return db.db.Delete(key, wopts)
}

// DeleteSync implements DB.
func (db PebbleDB) DeleteSync(key []byte) error {
	// fmt.Println("PebbleDB.DeleteSync")
	if len(key) == 0 {
		return errKeyEmpty
	}
	return db.db.Delete(key, pebble.Sync)
}

func (db *PebbleDB) DB() *pebble.DB {
	return db.db
}

// Close implements DB.
func (db PebbleDB) Close() error {
	// fmt.Println("PebbleDB.Close")
	db.db.Close()
	return nil
}

// Print implements DB.
func (db *PebbleDB) Print() error {
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (db *PebbleDB) Stats() map[string]string {
	/*
		keys := []string{"rocksdb.stats"}
		stats := make(map[string]string, len(keys))
		for _, key := range keys {
			stats[key] = db.(key)
		}
	*/
	return nil
}

// NewBatch implements DB.
func (db *PebbleDB) NewBatch() Batch {
	return newPebbleDBBatch(db)
}

// NewBatchWithSize implements DB.
// It does the same thing as NewBatch because we can't pre-allocate pebbleDBBatch
func (db *PebbleDB) NewBatchWithSize(size int) Batch {
	return newPebbleDBBatch(db)
}

// Iterator implements DB.
func (db *PebbleDB) Iterator(start, end []byte) (Iterator, error) {
	// fmt.Println("PebbleDB.Iterator")
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	o := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	itr, err := db.db.NewIter(&o)
	if err != nil {
		return nil, err
	}
	itr.First()

	return newPebbleDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *PebbleDB) ReverseIterator(start, end []byte) (Iterator, error) {
	// fmt.Println("PebbleDB.ReverseIterator")
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	o := pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}
	itr, err := db.db.NewIter(&o)
	if err != nil {
		return nil, err
	}
	itr.Last()
	return newPebbleDBIterator(itr, start, end, true), nil
}

var _ Batch = (*pebbleDBBatch)(nil)

type pebbleDBBatch struct {
	db    *PebbleDB
	batch *pebble.Batch
}

var _ Batch = (*pebbleDBBatch)(nil)

func newPebbleDBBatch(db *PebbleDB) *pebbleDBBatch {
	return &pebbleDBBatch{
		batch: db.db.NewBatch(),
	}
}

// Set implements Batch.
func (b *pebbleDBBatch) Set(key, value []byte) error {
	// fmt.Println("pebbleDBBatch.Set")
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.batch == nil {
		return errBatchClosed
	}
	b.batch.Set(key, value, nil)
	return nil
}

// Delete implements Batch.
func (b *pebbleDBBatch) Delete(key []byte) error {
	// fmt.Println("pebbleDBBatch.Delete")
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.batch == nil {
		return errBatchClosed
	}
	b.batch.Delete(key, nil)
	return nil
}

// Write implements Batch.
func (b *pebbleDBBatch) Write() error {
	// fmt.Println("pebbleDBBatch.Write")
	if b.batch == nil {
		return errBatchClosed
	}

	wopts := pebble.NoSync
	if isForceSync {
		wopts = pebble.Sync
	}
	err := b.batch.Commit(wopts)
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.

	return b.Close()
}

// WriteSync implements Batch.
func (b *pebbleDBBatch) WriteSync() error {
	// fmt.Println("pebbleDBBatch.WriteSync")
	if b.batch == nil {
		return errBatchClosed
	}
	err := b.batch.Commit(pebble.Sync)
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *pebbleDBBatch) Close() error {
	// fmt.Println("pebbleDBBatch.Close")
	if b.batch != nil {
		err := b.batch.Close()
		if err != nil {
			return err
		}
		b.batch = nil
	}

	return nil
}

// GetByteSize implements Batch
func (b *pebbleDBBatch) GetByteSize() (int, error) {
	if b.batch == nil {
		return 0, errBatchClosed
	}
	return b.batch.Len(), nil
}

type pebbleDBIterator struct {
	source     *pebble.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

var _ Iterator = (*pebbleDBIterator)(nil)

func newPebbleDBIterator(source *pebble.Iterator, start, end []byte, isReverse bool) *pebbleDBIterator {
	if isReverse {
		if end == nil {
			source.Last()
		}
	} else {
		if start == nil {
			source.First()
		}
	}
	return &pebbleDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Domain implements Iterator.
func (itr *pebbleDBIterator) Domain() ([]byte, []byte) {
	// fmt.Println("pebbleDBIterator.Domain")
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *pebbleDBIterator) Valid() bool {
	// fmt.Println("pebbleDBIterator.Valid")
	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// If source has error, invalid.
	if err := itr.source.Error(); err != nil {
		itr.isInvalid = true

		return false
	}

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true

		return false
	}

	// If key is end or past it, invalid.
	start := itr.start
	end := itr.end
	key := itr.source.Key()
	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true

			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true

			return false
		}
	}

	// It's valid.
	return true
}

// Key implements Iterator.
func (itr *pebbleDBIterator) Key() []byte {
	// fmt.Println("pebbleDBIterator.Key")
	itr.assertIsValid()
	return cp(itr.source.Key())
}

// Value implements Iterator.
func (itr *pebbleDBIterator) Value() []byte {
	// fmt.Println("pebbleDBIterator.Value")
	itr.assertIsValid()
	return cp(itr.source.Value())
}

// Next implements Iterator.
func (itr pebbleDBIterator) Next() {
	// fmt.Println("pebbleDBIterator.Next")
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

// Error implements Iterator.
func (itr *pebbleDBIterator) Error() error {
	return itr.source.Error()
}

// Close implements Iterator.
func (itr *pebbleDBIterator) Close() error {
	// fmt.Println("pebbleDBIterator.Close")
	return itr.source.Close()
}

func (itr *pebbleDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}

type fatalLogger struct {
	pebble.Logger
}

func (*fatalLogger) Fatalf(format string, args ...interface{}) {
	pebble.DefaultLogger.Fatalf(format, args...)
}

func (*fatalLogger) Infof(format string, args ...interface{}) {}
