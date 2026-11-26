package archiver

import (
	"context"
	stderrors "errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	rtest "github.com/restic/restic/internal/test"
)

func TestDetectReadChange(t *testing.T) {
	before := &fs.ExtendedFileInfo{Size: 10, ModTime: time.Unix(100, 0)}

	t.Run("unchanged", func(t *testing.T) {
		fake := &statOnlyFile{fi: &fs.ExtendedFileInfo{Size: 10, ModTime: time.Unix(100, 0)}}
		if change := detectReadChange("file", fake, before, nil); change != nil {
			t.Fatalf("unexpected change: %+v", change)
		}
	})

	t.Run("size and mtime changed", func(t *testing.T) {
		after := &fs.ExtendedFileInfo{Size: 20, ModTime: time.Unix(200, 0)}
		fake := &statOnlyFile{fi: after}
		change := detectReadChange("file", fake, before, nil)
		if change == nil || !change.Changed() {
			t.Fatalf("expected change, got %+v", change)
		}
		if change.After.Size != 20 {
			t.Fatalf("after size = %d, want 20", change.After.Size)
		}
	})

	t.Run("deleted after reading", func(t *testing.T) {
		dir := t.TempDir()
		missing := filepath.Join(dir, "missing")
		change := detectReadChange(missing, &statOnlyFile{err: os.ErrNotExist}, before, fs.NewLocal())
		if change == nil || !change.Deleted {
			t.Fatalf("expected deleted change, got %+v", change)
		}
	})
}

type statOnlyFile struct {
	fs.File
	fi  *fs.ExtendedFileInfo
	err error
}

func (f *statOnlyFile) Stat() (*fs.ExtendedFileInfo, error) { return f.fi, f.err }

func TestSnapshotAssociatesReadChangeWithNextSnapshot(t *testing.T) {
	ctx := context.Background()
	tempdir := rtest.TempDir(t)
	filename := filepath.Join(tempdir, "racy.txt")
	rtest.OK(t, os.WriteFile(filename, []byte("initial content"), 0o600))

	repo := repository.TestRepository(t)
	racyFS := &mutateOnReadFS{
		FS:     fs.NewLocal(),
		target: filename,
		append: []byte(" plus later write"),
	}

	arch := New(repo, racyFS, Options{ReadConcurrency: 1, MaxOpenFiles: 1})
	var (
		mu          sync.Mutex
		changeCount int
	)
	arch.Error = func(_ string, err error) error {
		var readChangeErr *ReadChangeError
		if stderrors.As(err, &readChangeErr) {
			mu.Lock()
			changeCount++
			mu.Unlock()
			return nil
		}
		t.Errorf("unexpected archiver error: %v", err)
		return err
	}

	_, firstID, _, err := arch.Snapshot(ctx, []string{filename}, SnapshotOptions{Time: time.Now()})
	rtest.OK(t, err)
	rtest.Assert(t, !firstID.IsNull(), "expected first snapshot ID")

	mu.Lock()
	gotWarnings := changeCount
	mu.Unlock()
	rtest.Equals(t, 1, gotWarnings)

	journal := arch.ChangeJournal.(*MemoryChangeJournal)
	pending, err := journal.PendingReadChanges(ctx)
	rtest.OK(t, err)
	rtest.Equals(t, 1, len(pending))
	rtest.Assert(t, pending[0].NextSnapshot == nil, "change must not be associated with current snapshot")

	// The next backup uses a normal filesystem and should consume the pending
	// observation by attaching the new snapshot ID.
	arch.FS = fs.NewLocal()
	_, secondID, _, err := arch.Snapshot(ctx, []string{filename}, SnapshotOptions{Time: time.Now()})
	rtest.OK(t, err)
	rtest.Assert(t, !secondID.IsNull(), "expected second snapshot ID")

	all := journal.changes
	rtest.Equals(t, 1, len(all))
	rtest.Assert(t, all[0].DetectedInSnapshot != nil && all[0].DetectedInSnapshot.Equal(firstID),
		"change should name detecting snapshot %v, got %+v", firstID, all[0].DetectedInSnapshot)
	rtest.Assert(t, all[0].NextSnapshot != nil && all[0].NextSnapshot.Equal(secondID),
		"change should be associated with next snapshot %v, got %+v", secondID, all[0].NextSnapshot)
	rtest.Assert(t, !all[0].NextSnapshot.Equal(firstID), "change was incorrectly associated with current snapshot")

	pending, err = journal.PendingReadChanges(ctx)
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(pending))
}

type mutateOnReadFS struct {
	fs.FS
	target string
	append []byte
}

func (m *mutateOnReadFS) OpenFile(name string, flags int, metadataOnly bool) (fs.File, error) {
	f, err := m.FS.OpenFile(name, flags, metadataOnly)
	if err != nil {
		return nil, err
	}
	if name == m.target {
		return &mutateOnReadFile{File: f, target: m.target, append: m.append}, nil
	}
	return f, nil
}

type mutateOnReadFile struct {
	fs.File
	target string
	append []byte
	once   sync.Once
}

func (f *mutateOnReadFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.once.Do(func() {
		handle, openErr := os.OpenFile(f.target, os.O_WRONLY|os.O_APPEND, 0o600)
		if openErr != nil {
			return
		}
		_, _ = handle.Write(f.append)
		_ = handle.Close()
		future := time.Now().Add(time.Hour)
		_ = os.Chtimes(f.target, future, future)
	})
	return n, err
}
