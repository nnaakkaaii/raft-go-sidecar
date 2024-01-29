package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"syscall"

	"github.com/nnaakkaaii/raft-go-sidecar/pkg/raft"
)

type FileStorage struct {
	filePath string
	file     *os.File
	mmap     []byte
}

func NewFileStorage(filePath string) *FileStorage {
	return &FileStorage{filePath: filePath}
}

var _ raft.Storage = (*FileStorage)(nil)

func (fs *FileStorage) Open() error {
	var err error
	fs.file, err = os.OpenFile(fs.filePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	fi, err := fs.file.Stat()
	if err != nil {
		return err
	}

	// ファイルサイズが0の場合、あらかじめ設定されたサイズ（例: 1KB）に拡張
	if fi.Size() == 0 {
		initialSize := int64(1 << 10) // 1KB
		if err := fs.file.Truncate(initialSize); err != nil {
			return err
		}
		fi, err = fs.file.Stat() // ファイルサイズを再取得
		if err != nil {
			return err
		}
	}

	fs.mmap, err = syscall.Mmap(int(fs.file.Fd()), 0, int(fi.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	return nil
}

func (fs *FileStorage) Close() error {
	if err := syscall.Munmap(fs.mmap); err != nil {
		return err
	}
	return fs.file.Close()
}

func (fs *FileStorage) LoadState(ctx context.Context, state *raft.State) {
	dec := gob.NewDecoder(bytes.NewReader(fs.mmap))
	dec.Decode(state)
}

func (fs *FileStorage) SaveState(ctx context.Context, state *raft.State) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(state)

	copy(fs.mmap, buf.Bytes())
}
