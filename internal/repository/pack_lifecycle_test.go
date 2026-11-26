package repository_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

var errInjected = errors.New("injected backend failure")

// flakyBackend wraps a memory backend and fails pack saves for the first
// failSaves calls. It counts both attempts and successful saves per handle.
type flakyBackend struct {
	backend.Backend
	mu           sync.Mutex
	failSaves    int32
	saveAttempts map[string]int
	saveSuccess  map[string]int
}

func newFlakyBackend() *flakyBackend {
	return &flakyBackend{
		Backend:      mem.New(),
		saveAttempts: make(map[string]int),
		saveSuccess:  make(map[string]int),
	}
}

func (f *flakyBackend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	f.mu.Lock()
	// only inject failures for pack uploads; repo init writes must succeed
	fail := h.Type == backend.PackFile && f.failSaves > 0
	if fail {
		f.failSaves--
	}
	f.saveAttempts[h.String()]++
	f.mu.Unlock()

	if fail {
		// drain the reader so a Rewind on retry works against the same data
		_ = rd.Rewind()
		return errInjected
	}
	if err := f.Backend.Save(ctx, h, rd); err != nil {
		return err
	}
	f.mu.Lock()
	f.saveSuccess[h.String()]++
	f.mu.Unlock()
	return nil
}

func (f *flakyBackend) attemptsFor(h backend.Handle) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveAttempts[h.String()]
}

func (f *flakyBackend) successFor(h backend.Handle) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveSuccess[h.String()]
}

// TestBackupRetriesFailedPackUpload verifies that transient pack upload
// failures are retried and that the backup completes without duplicate uploads
// or orphaned packs.
func TestBackupRetriesFailedPackUpload(t *testing.T) {
	be := newFlakyBackend()
	be.failSaves = 3
	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})
	repository.TestUseFastUploadBackoff(repo)
	ctx := context.Background()

	data := make([]byte, 256*1024)
	id := restic.Hash(data)

	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		sid, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, data, id, false)
		rtest.Assert(t, sid.Equal(id), "unexpected blob id")
		return err
	}))

	// the blob must be readable after the retried upload
	buf, err := repo.LoadBlob(ctx, restic.BlobHandle{Type: restic.DataBlob, ID: id}, nil)
	rtest.OK(t, err)
	rtest.Equals(t, len(data), len(buf))

	// every pack that ended up in the backend must have been saved exactly once
	var packCount int
	rtest.OK(t, repo.List(ctx, restic.PackFile, func(_ restic.ID, _ int64) error {
		packCount++
		return nil
	}))
	rtest.Assert(t, packCount > 0, "expected at least one uploaded pack")

	var totalSaves int
	for _, name := range packNames(t, repo, ctx) {
		h := backend.Handle{Type: backend.PackFile, Name: name}
		attempts := be.attemptsFor(h)
		success := be.successFor(h)
		rtest.Assert(t, success == 1, "pack %v stored %d times, expected exactly once", name, success)
		rtest.Assert(t, attempts >= 1, "pack %v has no save attempts", name)
		totalSaves += success
	}
	rtest.Assert(t, totalSaves == packCount, "every stored pack must map to exactly one successful save")
}

// silentSuccessBackend stores data successfully but reports an error on the
// first save of each pack, simulating a lost response from the backend.
type silentSuccessBackend struct {
	*mem.MemoryBackend
	mu       sync.Mutex
	failed   map[string]struct{}
	statHits atomic.Int32
}

func newSilentSuccessBackend() *silentSuccessBackend {
	return &silentSuccessBackend{
		MemoryBackend: mem.New(),
		failed:        make(map[string]struct{}),
	}
}

func (s *silentSuccessBackend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	// always store first, mimicking a backend that received the full object
	if err := s.MemoryBackend.Save(ctx, h, rd); err != nil {
		return err
	}
	if h.Type != backend.PackFile {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, done := s.failed[h.String()]; !done {
		s.failed[h.String()] = struct{}{}
		return errInjected
	}
	return nil
}

func (s *silentSuccessBackend) Stat(ctx context.Context, h backend.Handle) (backend.FileInfo, error) {
	s.statHits.Add(1)
	return s.MemoryBackend.Stat(ctx, h)
}

// TestIdempotentPackUploadAfterSilentSuccess simulates an upload that actually
// stored the pack but returned an error (lost response). The retry must detect
// the existing object via Stat and register it in the index instead of
// re-uploading.
func TestIdempotentPackUploadAfterSilentSuccess(t *testing.T) {
	be := newSilentSuccessBackend()
	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})
	repository.TestUseFastUploadBackoff(repo)
	ctx := context.Background()

	data := make([]byte, 128*1024)
	id := restic.Hash(data)

	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		_, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, data, id, false)
		return err
	}))

	// despite the lost response, exactly one pack exists and the blob is indexed
	packs := packNames(t, repo, ctx)
	rtest.Assert(t, len(packs) == 1, "expected one pack, got %d", len(packs))
	rtest.Assert(t, be.statHits.Load() > 0, "retry should have detected the existing pack via Stat")

	buf, err := repo.LoadBlob(ctx, restic.BlobHandle{Type: restic.DataBlob, ID: id}, nil)
	rtest.OK(t, err)
	rtest.Equals(t, len(data), len(buf))

	// orphan cleanup must leave the indexed pack untouched
	rtest.OK(t, repo.CleanupOrphanPacks(ctx))
	rtest.Equals(t, packs, packNames(t, repo, ctx))
}

// TestOrphanedPackFromInterruptedBackup verifies that a pack present in the
// backend but absent from the index is detected and removed on the next run.
func TestOrphanedPackFromInterruptedBackup(t *testing.T) {
	be := mem.New()
	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})
	ctx := context.Background()

	// a pack written by a previous backup that crashed before the index flush
	orphanID := restic.NewRandomID()
	h := backend.Handle{Type: backend.PackFile, Name: orphanID.String()}
	rtest.OK(t, be.Save(ctx, h, backend.NewByteReader([]byte("orphaned pack contents"), be.Hasher())))

	// a regular backup creates an indexed pack
	data := make([]byte, 64*1024)
	blobID := restic.Hash(data)
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		_, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, data, blobID, false)
		return err
	}))

	rtest.OK(t, repo.CleanupOrphanPacks(ctx))

	_, err := be.Stat(ctx, h)
	rtest.Assert(t, be.IsNotExist(err), "orphaned pack should have been removed")

	// indexed data stays readable
	buf, err := repo.LoadBlob(ctx, restic.BlobHandle{Type: restic.DataBlob, ID: blobID}, nil)
	rtest.OK(t, err)
	rtest.Equals(t, len(data), len(buf))
}

// TestFlushUploadsUnderfilledPack verifies that a pack below the size limit is
// uploaded and indexed when the uploader is flushed.
func TestFlushUploadsUnderfilledPack(t *testing.T) {
	be := mem.New()
	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})
	ctx := context.Background()

	data := []byte("a small blob that does not fill a pack")
	id := restic.Hash(data)

	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		_, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, data, id, false)
		return err
	}))

	// after WithBlobUploader returns, the underfilled pack must have been
	// flushed and its blob indexed
	buf, err := repo.LoadBlob(ctx, restic.BlobHandle{Type: restic.DataBlob, ID: id}, nil)
	rtest.OK(t, err)
	rtest.Assert(t, string(buf) == string(data), "blob content mismatch after flush")

	rtest.Assert(t, len(packNames(t, repo, ctx)) == 1, "underfilled pack must have been uploaded on flush")
}

func packNames(t testing.TB, repo *repository.Repository, ctx context.Context) []string {
	t.Helper()
	var names []string
	rtest.OK(t, repo.List(ctx, restic.PackFile, func(id restic.ID, _ int64) error {
		names = append(names, id.String())
		return nil
	}))
	return names
}
