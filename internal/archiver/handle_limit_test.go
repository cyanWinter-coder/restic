package archiver

import (
	"context"
	stderrors "errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	rtest "github.com/restic/restic/internal/test"
)

func TestMaxOpenFilesQueuesFileReads(t *testing.T) {
	files := TestDir{}
	for i := range 8 {
		files["file-"+string(rune('a'+i))] = TestFile{Content: "payload-payload-payload"}
	}
	tempdir := rtest.TempDir(t)
	TestCreateFiles(t, tempdir, files)

	repo := repository.TestRepository(t)
	trackFS := &handleCountFS{
		FS:      fs.NewLocal(),
		gate:    make(chan struct{}),
		started: make(chan struct{}, 16),
	}

	const limit = 2
	arch := New(repo, trackFS, Options{
		ReadConcurrency: 8,
		MaxOpenFiles:    limit,
	})

	snapshotDone := make(chan error, 1)
	go func() {
		_, _, _, err := arch.Snapshot(context.Background(), []string{tempdir}, SnapshotOptions{Time: time.Now()})
		snapshotDone <- err
	}()

	for range limit {
		select {
		case <-trackFS.started:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for file reads to start")
		}
	}

	// Give an incorrectly unbounded implementation a chance to overshoot.
	time.Sleep(50 * time.Millisecond)
	if active := trackFS.active.Load(); active != limit {
		t.Fatalf("active readable handles = %d, want %d", active, limit)
	}
	if max := trackFS.maxActive.Load(); max > limit {
		t.Fatalf("max readable handles = %d, want at most %d", max, limit)
	}

	close(trackFS.gate)
	select {
	case err := <-snapshotDone:
		rtest.OK(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("snapshot did not finish after releasing blocked reads")
	}

	if max := trackFS.maxActive.Load(); max > limit {
		t.Fatalf("observed %d concurrently open files, limit is %d", max, limit)
	}

	poolDone := make(chan struct{})
	go func() { arch.handleSlots.Wait(); close(poolDone) }()
	select {
	case <-poolDone:
	case <-time.After(time.Second):
		t.Fatal("handle pool slots were not all released after snapshot")
	}
}

func TestCancellationStopsQueuedFilesAndDrainsHandles(t *testing.T) {
	files := TestDir{}
	for i := range 6 {
		files["cancel-file-"+string(rune('a'+i))] = TestFile{Content: "payload-payload-payload"}
	}
	tempdir := rtest.TempDir(t)
	TestCreateFiles(t, tempdir, files)

	repo := repository.TestRepository(t)
	trackFS := &handleCountFS{
		FS:      fs.NewLocal(),
		gate:    make(chan struct{}),
		started: make(chan struct{}, 16),
	}

	const limit = 2
	arch := New(repo, trackFS, Options{ReadConcurrency: 6, MaxOpenFiles: limit})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapshotDone := make(chan error, 1)
	go func() {
		_, _, _, err := arch.Snapshot(ctx, []string{tempdir}, SnapshotOptions{Time: time.Now()})
		snapshotDone <- err
	}()

	for range limit {
		select {
		case <-trackFS.started:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for active file reads")
		}
	}

	cancel()
	close(trackFS.gate)

	select {
	case err := <-snapshotDone:
		if err != nil && !stderrors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("active file tasks did not converge after cancellation")
	}

	if started := trackFS.startedCount.Load(); started > limit {
		t.Fatalf("started %d file reads after cancellation/queueing, want at most %d", started, limit)
	}

	poolDone := make(chan struct{})
	go func() { arch.handleSlots.Wait(); close(poolDone) }()
	select {
	case <-poolDone:
	case <-time.After(time.Second):
		t.Fatal("handles from canceled snapshot were not released")
	}
}

type handleCountFS struct {
	fs.FS
	active       atomic.Int32
	maxActive    atomic.Int32
	startedCount atomic.Int32
	gate         chan struct{}
	started      chan struct{}
}

type handleCountFile struct {
	fs.File
	owner *handleCountFS
	once  sync.Once
}

func (f *handleCountFS) OpenFile(name string, flags int, metadataOnly bool) (fs.File, error) {
	file, err := f.FS.OpenFile(name, flags, metadataOnly)
	if err != nil {
		return nil, err
	}
	return &handleCountFile{File: file, owner: f}, nil
}

func (f *handleCountFile) MakeReadable() error {
	if err := f.File.MakeReadable(); err != nil {
		return err
	}
	active := f.owner.active.Add(1)
	for {
		max := f.owner.maxActive.Load()
		if active <= max || f.owner.maxActive.CompareAndSwap(max, active) {
			break
		}
	}
	return nil
}

func (f *handleCountFile) Read(p []byte) (int, error) {
	f.once.Do(func() {
		f.owner.startedCount.Add(1)
		f.owner.started <- struct{}{}
		<-f.owner.gate
	})
	return f.File.Read(p)
}

func (f *handleCountFile) Close() error {
	err := f.File.Close()
	f.owner.active.Add(-1)
	return err
}
