package log

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nnaakkaaii/raft-go-sidecar/pkg/raft"
)

func setupLevelDBLogStorage(t *testing.T) (*LevelDBLogStorage, func()) {
	t.Helper()

	// 一時的なディレクトリを作成
	dir, err := os.MkdirTemp("", "leveldbtest")
	assert.NoError(t, err)

	ls, err := NewLevelDBLogStorage(dir, 1)
	assert.NoError(t, err)
	return ls, func() {
		ls.Close()
		os.RemoveAll(dir)
	}
}

func TestAppend(t *testing.T) {
	ls, cleanup := setupLevelDBLogStorage(t)
	defer cleanup()

	ctx := context.Background()
	entry := raft.Entry{Command: "cmd1", Index: 1, Term: 1}
	index := ls.Append(ctx, entry)
	assert.Equal(t, entry.Index, index)

	// 追加されたエントリを検証
	foundEntry := ls.Find(ctx, index)
	assert.Equal(t, entry, foundEntry)
}

func TestExtend(t *testing.T) {
	ls, cleanup := setupLevelDBLogStorage(t)
	defer cleanup()

	ctx := context.Background()
	entries := []raft.Entry{
		{Command: "cmd1", Index: 1, Term: 1},
		{Command: "cmd2", Index: 2, Term: 1},
	}
	lastIndex := ls.Extend(ctx, entries, 1)
	assert.Equal(t, entries[1].Index, lastIndex)

	// 拡張されたエントリを検証
	for _, entry := range entries {
		foundEntry := ls.Find(ctx, entry.Index)
		assert.Equal(t, entry, foundEntry)
	}
}

func TestFind(t *testing.T) {
	ls, cleanup := setupLevelDBLogStorage(t)
	defer cleanup()

	ctx := context.Background()
	entry := raft.Entry{Command: "cmd1", Index: 1, Term: 1}
	ls.Append(ctx, entry)

	foundEntry := ls.Find(ctx, entry.Index)
	assert.Equal(t, entry, foundEntry)
}

func TestSlice(t *testing.T) {
	ls, cleanup := setupLevelDBLogStorage(t)
	defer cleanup()

	ctx := context.Background()
	entries := []raft.Entry{
		{Command: "cmd1", Index: 1, Term: 1},
		{Command: "cmd2", Index: 2, Term: 1},
		{Command: "cmd3", Index: 3, Term: 1},
	}
	for _, entry := range entries {
		ls.Append(ctx, entry)
	}

	slicedEntries := ls.Slice(ctx, 2)
	assert.Equal(t, 2, len(slicedEntries))
	assert.Equal(t, entries[1:], slicedEntries)
}
