package db

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cast"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	// GoLevelDB tuning constants optimized for Cosmos SDK workloads.
	// All options are backwards compatible with existing databases.

	// goleveldbBloomFilterBits is bits per key for bloom filter.
	// Already set in current implementation (10 bits = ~1% false positive rate).
	goleveldbBloomFilterBits = 10

	// goleveldbOpenFilesCacheCapacity is the number of open file handles.
	// Increased from default (500) to reduce file handle churn.
	goleveldbOpenFilesCacheCapacity = 1024

	// goleveldbCompactionTableSize is the target size for SST files (64 MB).
	// Larger than default (2 MB) to reduce file count and improve read performance.
	// Safe: new files only, old files remain readable.
	goleveldbCompactionTableSize = 64 << 20

	// goleveldbCompactionTotalSizeMultiplier controls level size growth.
	// Higher than default (10) for less aggressive compaction.
	goleveldbCompactionTotalSizeMultiplier = 15.0

	// goleveldbWriteBufferMin is the minimum write buffer size (64 MB).
	goleveldbWriteBufferMin = 64 << 20

	// goleveldbCacheFallback is the fallback cache size when RAM detection fails (4 GB).
	goleveldbCacheFallback = 4 << 30

	// goleveldbCacheMax is the maximum cache size to prevent excessive GC pauses.
	// 16 GB provides good performance while keeping GC pauses under 400ms.
	goleveldbCacheMax = 16 << 30
)

func init() {
	dbCreator := func(name string, dir string, opts Options) (DB, error) {
		return NewGoLevelDB(name, dir, opts)
	}
	registerDBCreator(GoLevelDBBackend, dbCreator, false)
}

type GoLevelDB struct {
	db *leveldb.DB
}

var _ DB = (*GoLevelDB)(nil)

func NewGoLevelDB(name string, dir string, opts Options) (*GoLevelDB, error) {
	// Detect system resources for auto-tuning
	sysRes := GetSystemResources()

	// Calculate cache size: 1/3 of RAM, capped at 16 GB to limit GC pauses
	cacheSize := sysRes.TotalRAM / 3
	if cacheSize == 0 {
		cacheSize = goleveldbCacheFallback
	}
	if cacheSize > goleveldbCacheMax {
		cacheSize = goleveldbCacheMax
	}

	// Calculate write buffer: cache / 4, minimum 64 MB
	writeBuffer := cacheSize / 4
	if writeBuffer < goleveldbWriteBufferMin {
		writeBuffer = goleveldbWriteBufferMin
	}

	defaultOpts := &opt.Options{
		// Essential: Bloom filter for read performance
		// Safe: applies to new SST files only, old files work without filter
		Filter: filter.NewBloomFilter(goleveldbBloomFilterBits),

		// Auto-tuned based on system resources (runtime only, always safe)
		BlockCacheCapacity: int(cacheSize),
		WriteBuffer:        int(writeBuffer),

		// Fixed optimizations (all backwards compatible)
		OpenFilesCacheCapacity:        goleveldbOpenFilesCacheCapacity,
		CompactionTableSize:           goleveldbCompactionTableSize,
		CompactionTotalSizeMultiplier: goleveldbCompactionTotalSizeMultiplier,
	}

	// Override from user-provided options
	if opts != nil {
		if files := cast.ToInt(opts.Get("maxopenfiles")); files > 0 {
			defaultOpts.OpenFilesCacheCapacity = files
		}
		if cache := cast.ToInt(opts.Get("cache")); cache > 0 {
			defaultOpts.BlockCacheCapacity = cache << 20 // MB to bytes
		}
		if wb := cast.ToInt(opts.Get("writebuffer")); wb > 0 {
			defaultOpts.WriteBuffer = wb << 20 // MB to bytes
		}
	}

	return NewGoLevelDBWithOpts(name, dir, defaultOpts)
}

func NewGoLevelDBWithOpts(name string, dir string, o *opt.Options) (*GoLevelDB, error) {
	dbPath := filepath.Join(dir, name+DBFileSuffix)
	db, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		return nil, err
	}
	database := &GoLevelDB{
		db: db,
	}
	return database, nil
}

// Get implements DB.
func (db *GoLevelDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	res, err := db.db.Get(key, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}

// Has implements DB.
func (db *GoLevelDB) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *GoLevelDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if err := db.db.Put(key, value, nil); err != nil {
		return err
	}
	return nil
}

// SetSync implements DB.
func (db *GoLevelDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if err := db.db.Put(key, value, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}
	return nil
}

// Delete implements DB.
func (db *GoLevelDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if err := db.db.Delete(key, nil); err != nil {
		return err
	}
	return nil
}

// DeleteSync implements DB.
func (db *GoLevelDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.db.Delete(key, &opt.WriteOptions{Sync: true})
	if err != nil {
		return err
	}
	return nil
}

func (db *GoLevelDB) DB() *leveldb.DB {
	return db.db
}

// Close implements DB.
func (db *GoLevelDB) Close() error {
	if err := db.db.Close(); err != nil {
		return err
	}
	return nil
}

// Print implements DB.
func (db *GoLevelDB) Print() error {
	str, err := db.db.GetProperty("leveldb.stats")
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", str)

	itr := db.db.NewIterator(nil, nil)
	for itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (db *GoLevelDB) Stats() map[string]string {
	keys := []string{
		"leveldb.num-files-at-level{n}",
		"leveldb.stats",
		"leveldb.sstables",
		"leveldb.blockpool",
		"leveldb.cachedblock",
		"leveldb.openedtables",
		"leveldb.alivesnaps",
		"leveldb.aliveiters",
	}

	stats := make(map[string]string)
	for _, key := range keys {
		str, err := db.db.GetProperty(key)
		if err == nil {
			stats[key] = str
		}
	}
	return stats
}

func (db *GoLevelDB) ForceCompact(start, limit []byte) error {
	return db.db.CompactRange(util.Range{Start: start, Limit: limit})
}

// NewBatch implements DB.
func (db *GoLevelDB) NewBatch() Batch {
	return newGoLevelDBBatch(db)
}

// NewBatchWithSize implements DB.
func (db *GoLevelDB) NewBatchWithSize(size int) Batch {
	return newGoLevelDBBatchWithSize(db, size)
}

// Iterator implements DB.
func (db *GoLevelDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr := db.db.NewIterator(&util.Range{Start: start, Limit: end}, nil)
	return newGoLevelDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *GoLevelDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr := db.db.NewIterator(&util.Range{Start: start, Limit: end}, nil)
	return newGoLevelDBIterator(itr, start, end, true), nil
}
