package log

import (
	"context"
	"encoding/gob"
	"os"
	"sync"

	"github.com/nnaakkaaii/raft-actor-model/pkg/raft"
)

type FileLogStorage struct {
	filePath     string
	file         *os.File
	mu           sync.Mutex
	cache        []raft.Entry
	maxCacheSize int
}

func NewFileLogStorage(filePath string, maxCacheSize int) *FileLogStorage {
	return &FileLogStorage{
		filePath:     filePath,
		maxCacheSize: maxCacheSize,
	}
}

var _ raft.Log = (*FileLogStorage)(nil)

func (fls *FileLogStorage) openFile() error {
	if fls.file == nil {
		var err error
		fls.file, err = os.OpenFile(fls.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fls *FileLogStorage) Append(ctx context.Context, entry raft.Entry) int32 {
	fls.mu.Lock()
	defer fls.mu.Unlock()

	if err := fls.openFile(); err != nil {
		return -1
	}

	enc := gob.NewEncoder(fls.file)
	if err := enc.Encode(entry); err != nil {
		return -1
	}

	fls.cache = append(fls.cache, entry)
	if len(fls.cache) > fls.maxCacheSize {
		fls.cache = fls.cache[1:]
	}

	return entry.Index
}

func (fls *FileLogStorage) Extend(ctx context.Context, entries []raft.Entry, from int32) int32 {
	fls.mu.Lock()
	defer fls.mu.Unlock()

	if err := fls.openFile(); err != nil {
		return -1
	}

	enc := gob.NewEncoder(fls.file)
	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return -1
		}
	}

	fls.cache = append(fls.cache, entries...)
	if len(fls.cache) > fls.maxCacheSize {
		fls.cache = fls.cache[len(fls.cache)-fls.maxCacheSize:]
	}

	return entries[len(entries)-1].Index
}

func (fls *FileLogStorage) Find(ctx context.Context, index int32) raft.Entry {
	fls.mu.Lock()
	defer fls.mu.Unlock()

	// Check cache first
	for i := len(fls.cache) - 1; i >= 0; i-- {
		if fls.cache[i].Index == index {
			return fls.cache[i]
		}
	}

	// Read from file
	file, err := os.Open(fls.filePath)
	if err != nil {
		return raft.Entry{}
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	var entry raft.Entry
	for {
		if err := dec.Decode(&entry); err != nil {
			break
		}
		if entry.Index == index {
			return entry
		}
	}

	return raft.Entry{}
}

func (fls *FileLogStorage) Slice(ctx context.Context, from int32) []raft.Entry {
	fls.mu.Lock()
	defer fls.mu.Unlock()

	entries := make([]raft.Entry, 0)

	// Read from cache
	for _, entry := range fls.cache {
		if entry.Index >= from {
			entries = append(entries, entry)
		}
	}

	return entries
}
