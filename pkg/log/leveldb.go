package log

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/nnaakkaaii/raft-actor-model/pkg/raft"
)

type LevelDBLogStorage struct {
	db    *leveldb.DB
	cache map[int32]raft.Entry
	keys  []int32
	limit int
	mu    sync.Mutex
}

func NewLevelDBLogStorage(dbPath string, limit int) (*LevelDBLogStorage, error) {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDBLogStorage{
		db:    db,
		cache: make(map[int32]raft.Entry),
		keys:  make([]int32, 0),
		limit: limit,
	}, nil
}

func (ls *LevelDBLogStorage) Append(ctx context.Context, entry raft.Entry) int32 {
	data, err := encodeEntry(entry)
	if err != nil {
		return -1
	}
	err = ls.db.Put(encodeIndex(entry.Index), data, nil)
	if err != nil {
		return -1
	}

	ls.updateCache(entry.Index, entry)

	return entry.Index
}

func (ls *LevelDBLogStorage) Extend(ctx context.Context, entries []raft.Entry, from int32) int32 {
	batch := new(leveldb.Batch)

	iter := ls.db.NewIterator(nil, nil)
	for iter.Seek(encodeIndex(from)); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()

	for _, index := range ls.keys {
		if index >= from {
			delete(ls.cache, index)
		}
	}
	ls.keys = filterKeys(ls.keys, from)

	for _, entry := range entries {
		data, err := encodeEntry(entry)
		if err != nil {
			return -1
		}
		batch.Put(encodeIndex(entry.Index), data)
		ls.updateCache(entry.Index, entry)
	}

	err := ls.db.Write(batch, nil)
	if err != nil {
		return -1
	}

	if len(entries) == 0 {
		return from
	}

	return entries[len(entries)-1].Index
}

func (ls *LevelDBLogStorage) Find(ctx context.Context, index int32) raft.Entry {
	data, err := ls.db.Get(encodeIndex(index), nil)

	if entry, ok := ls.cache[index]; ok {
		return entry
	}

	if err != nil {
		return raft.Entry{}
	}
	entry, _ := decodeEntry(data)
	return entry
}

func (ls *LevelDBLogStorage) Slice(ctx context.Context, from int32) []raft.Entry {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	var entries []raft.Entry
	for _, index := range ls.keys {
		if index >= from {
			if entry, ok := ls.cache[index]; ok {
				entries = append(entries, entry)
			} else {
				break
			}
		}
	}

	iter := ls.db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Seek(encodeIndex(from)); iter.Valid(); iter.Next() {
		index := decodeIndex(iter.Key())
		if _, ok := ls.cache[index]; !ok {
			entry, _ := decodeEntry(iter.Value())
			entries = append(entries, entry)
		}
	}

	return entries
}

func encodeIndex(index int32) []byte {
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(index)
	return buf.Bytes()
}

func decodeIndex(data []byte) int32 {
	var index int32
	buf := bytes.NewBuffer(data)
	gob.NewDecoder(buf).Decode(&index)
	return index
}

func encodeEntry(entry raft.Entry) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(entry)
	return buf.Bytes(), err
}

func decodeEntry(data []byte) (raft.Entry, error) {
	var entry raft.Entry
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(&entry)
	return entry, err
}

func (ls *LevelDBLogStorage) updateCache(index int32, entry raft.Entry) {
	ls.cache[index] = entry
	ls.keys = append(ls.keys, index)

	if len(ls.cache) > ls.limit {
		delete(ls.cache, ls.keys[0])
		ls.keys = ls.keys[1:]
	}
}

func filterKeys(keys []int32, from int32) []int32 {
	var filtered []int32
	for _, key := range keys {
		if key < from {
			filtered = append(filtered, key)
		}
	}
	return filtered
}

func (ls *LevelDBLogStorage) Close() error {
	return ls.db.Close()
}
