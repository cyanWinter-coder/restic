package repository

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/fileio"
	"github.com/restic/restic/internal/repository/crypto"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/test"
	"golang.org/x/sync/errgroup"
)

// noBackoff keeps retry tests fast and deterministic.
func noBackoff(_ uint) time.Duration { return time.Millisecond }

// countingSavePacker records how often savePacker is invoked and fails the
// first failCount attempts.
type countingSavePacker struct {
	mu        sync.Mutex
	calls     atomic.Int32
	failCount int32
	failErr   error
	block     <-chan struct{}
}

func (c *countingSavePacker) savePacker(_ context.Context, _ restic.BlobType, _ *packer) error {
	if c.block != nil {
		<-c.block
	}
	n := c.calls.Add(1)
	if n <= c.failCount {
		return c.failErr
	}
	return nil
}

func TestPackerUploaderRetriesUntilSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, wgCtx := errgroup.WithContext(ctx)
	repo := &countingSavePacker{failCount: 2, failErr: errors.New("transient upload failure")}
	pu := newPackerUploader(wgCtx, wg, repo, 1)
	pu.backoff = noBackoff

	p := &packer{}
	test.OK(t, pu.QueuePacker(ctx, restic.DataBlob, p))

	pu.TriggerShutdown()
	test.OK(t, wg.Wait())
	test.Equals(t, int32(3), repo.calls.Load())
}

func TestPackerUploaderExhaustsRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, wgCtx := errgroup.WithContext(ctx)
	repo := &countingSavePacker{failCount: 100, failErr: errors.New("permanent upload failure")}
	pu := newPackerUploader(wgCtx, wg, repo, 1)
	pu.backoff = noBackoff

	p := &packer{}
	test.OK(t, pu.QueuePacker(ctx, restic.DataBlob, p))

	pu.TriggerShutdown()
	err := wg.Wait()
	test.Assert(t, err != nil, "expected the upload error to be returned")
	// initial attempt plus maxRetries retries
	test.Equals(t, int32(pu.maxRetries+1), repo.calls.Load())
}

func TestPackerUploaderDeduplicatesQueuedPacker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	block := make(chan struct{})
	repo := &countingSavePacker{block: block}

	wg, wgCtx := errgroup.WithContext(ctx)
	pu := newPackerUploader(wgCtx, wg, repo, 1)
	pu.backoff = noBackoff

	p := &packer{}
	test.OK(t, pu.QueuePacker(ctx, restic.DataBlob, p))

	// while the first upload is blocked, repeated requests must be no-ops
	for range 5 {
		test.OK(t, pu.QueuePacker(ctx, restic.DataBlob, p))
	}
	close(block)

	pu.TriggerShutdown()
	test.OK(t, wg.Wait())
	test.Equals(t, int32(1), repo.calls.Load())
}

func TestPackerManagerRegistryReturnsSameHandle(t *testing.T) {
	pm := newPackerManager(nil, restic.DataBlob, DefaultPackSize, defaultPackerCount,
		func(_ context.Context, _ restic.BlobType, _ *packer) error { return nil })

	// fill the rotation slots and check that every created handle is registered exactly once
	seen := map[*packer]struct{}{}
	for range defaultPackerCount {
		p, err := pm.newPacker()
		test.OK(t, err)
		seen[p] = struct{}{}
		pm.packers = append(pm.packers, p)
	}

	pm.pm.Lock()
	test.Equals(t, len(seen), len(pm.registry))
	for p := range seen {
		_, ok := pm.registry[p]
		test.Assert(t, ok, "packer handle %p missing from registry", p)
	}
	pm.pm.Unlock()

	// detaching a handle removes it from the rotation but keeps it registered
	var one *packer
	for p := range seen {
		one = p
		break
	}
	pm.detachPacker(one)
	pm.pm.Lock()
	_, ok := pm.registry[one]
	pm.pm.Unlock()
	test.Assert(t, ok, "detached packer must remain registered until the uploader takes it")

	// discard releases every remaining handle without uploading
	pm.pm.Lock()
	pm.discard()
	test.Equals(t, 0, len(pm.registry))
	pm.pm.Unlock()
}

func TestPackerUploaderDiscardPendingReleasesHandles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, wgCtx := errgroup.WithContext(ctx)
	repo := &countingSavePacker{failCount: 100, failErr: errors.New("permanent upload failure")}
	pu := newPackerUploader(wgCtx, wg, repo, 1)
	pu.backoff = noBackoff

	tmp, err := fileio.TempFile("", "restic-test-pack-")
	test.OK(t, err)
	tmpName := tmp.Name()
	p := &packer{tmpfile: tmp}

	test.OK(t, pu.QueuePacker(ctx, restic.DataBlob, p))
	pu.TriggerShutdown()
	err = wg.Wait()
	test.Assert(t, err != nil, "expected upload to fail")

	// the failed packer is still tracked until DiscardPending runs
	pu.mu.Lock()
	_, tracked := pu.inFlight[p]
	pu.mu.Unlock()
	test.Assert(t, tracked, "failed packer should remain tracked for cleanup")

	pu.DiscardPending()
	test.Equals(t, 0, len(pu.inFlight))
	_, statErr := os.Stat(tmpName)
	test.Assert(t, os.IsNotExist(statErr), "temp file should have been released, got %v", statErr)
}

func TestPackerManagerFlushClearsRegistry(t *testing.T) {
	queued := make(chan *packer, 8)
	pm := newPackerManager(crypto.NewRandomKey(), restic.DataBlob, DefaultPackSize, defaultPackerCount,
		func(_ context.Context, _ restic.BlobType, p *packer) error {
			queued <- p
			return nil
		})

	for range 4 {
		_, err := pm.SaveBlob(context.Background(), restic.DataBlob, restic.NewRandomID(), []byte("blob"), 0)
		test.OK(t, err)
	}
	test.OK(t, pm.Flush(context.Background()))

	pm.pm.Lock()
	test.Equals(t, 0, len(pm.registry))
	pm.pm.Unlock()
	close(queued)
	test.Assert(t, len(queued) > 0, "expected at least one queued pack")
}

func TestPackerManagerDiscardsPackAfterQueueFailure(t *testing.T) {
	queueErr := errors.New("queue closed")
	pm := newPackerManager(crypto.NewRandomKey(), restic.DataBlob, 256*1024, defaultPackerCount,
		func(_ context.Context, _ restic.BlobType, _ *packer) error { return queueErr })

	// write a blob large enough to fill the pack so that SaveBlob tries to queue it
	buf := make([]byte, 300*1024)
	_, err := pm.SaveBlob(context.Background(), restic.DataBlob, restic.NewRandomID(), buf, 0)
	test.Assert(t, errors.Is(err, queueErr), "expected queue error, got %v", err)

	// the failed packer is no longer in rotation but is still tracked, so discard can release it
	pm.pm.Lock()
	test.Assert(t, len(pm.registry) == 1, "expected one tracked handle, got %d", len(pm.registry))
	pm.discard()
	test.Equals(t, 0, len(pm.registry))
	pm.pm.Unlock()
}

func TestCleanupOrphanPacks(t *testing.T) {
	repo, _ := TestRepositoryWithBackend(t, mem.New(), 2, Options{})
	ctx := context.Background()

	// simulate two packs from a previous backup: one referenced by the index,
	// one orphaned
	indexed := restic.NewRandomID()
	orphan := restic.NewRandomID()

	for _, id := range []restic.ID{indexed, orphan} {
		h := backend.Handle{Type: backend.PackFile, Name: id.String()}
		test.OK(t, repo.be.Save(ctx, h, backend.NewByteReader([]byte("pack data"), repo.be.Hasher())))
	}

	// register the indexed pack in the in-memory index
	test.OK(t, repo.idx.StorePack(ctx, indexed, nil, &internalRepository{repo}))

	test.OK(t, repo.CleanupOrphanPacks(ctx))

	_, err := repo.be.Stat(ctx, backend.Handle{Type: backend.PackFile, Name: indexed.String()})
	test.Assert(t, err == nil, "indexed pack must be kept, got %v", err)

	_, err = repo.be.Stat(ctx, backend.Handle{Type: backend.PackFile, Name: orphan.String()})
	test.Assert(t, repo.be.IsNotExist(err), "orphaned pack must have been removed")

	// a second run is a no-op and must not fail
	test.OK(t, repo.CleanupOrphanPacks(ctx))
}
