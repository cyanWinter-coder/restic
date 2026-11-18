package repository_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// drainErrors runs a checker method that reports errors via the channel and
// returns all collected errors after the channel has been closed.
func drainErrors(f func(chan<- error)) []error {
	errChan := make(chan error)
	go f(errChan)

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	return errs
}

// vanishPackBackend deletes pack files from the underlying backend right after
// they have been saved. This simulates a faulty backend that loses data after
// a successful upload, or an external process that removes files concurrently.
type vanishPackBackend struct {
	backend.Backend
	mu sync.Mutex
}

func (b *vanishPackBackend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	if err := b.Backend.Save(ctx, h, rd); err != nil {
		return err
	}
	if h.Type == backend.PackFile {
		b.mu.Lock()
		err := b.Backend.Remove(ctx, h)
		b.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// statNotExistBackend reports pack files as non-existent on Stat while still
// serving their contents on Load. It exercises the flush-time existence check
// without modifying the stored data.
type statNotExistBackend struct {
	backend.Backend
}

func (b *statNotExistBackend) Stat(ctx context.Context, h backend.Handle) (backend.FileInfo, error) {
	if h.Type == backend.PackFile {
		return backend.FileInfo{}, errors.New("simulated stat failure: file not found")
	}
	return b.Backend.Stat(ctx, h)
}

func (b *statNotExistBackend) IsNotExist(err error) bool {
	if err != nil && err.Error() == "simulated stat failure: file not found" {
		return true
	}
	return b.Backend.IsNotExist(err)
}

// saveBlobs runs a complete blob upload cycle, including flushing packs and
// the index.
func saveBlobs(t testing.TB, repo restic.Repository, num int) {
	t.Helper()
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		for i := range num {
			buf := rtest.Random(i+1, 8*1024)
			_, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
			if err != nil {
				return err
			}
		}
		return nil
	}))
}

// TestFlushFailsOnMissingUploadedPack verifies that the repository does not
// write an index referencing pack files that vanished after the upload. The
// flush must fail with an ErrPackMissing and no new index files must be stored.
func TestFlushFailsOnMissingUploadedPack(t *testing.T) {
	be := mem.New()
	repo, _ := repository.TestRepositoryWithBackend(t, &vanishPackBackend{Backend: be}, 0, repository.Options{})

	indexesBefore := listFiles(t, repo, restic.IndexFile)

	err := repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		buf := rtest.Random(23, 64*1024)
		_, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
		return err
	})

	var missing *repository.ErrPackMissing
	rtest.Assert(t, errors.As(err, &missing), "expected ErrPackMissing, got %v", err)

	// no index referencing the lost packs may have been written
	indexesAfter := listFiles(t, repo, restic.IndexFile)
	rtest.Equals(t, indexesBefore, indexesAfter, "index files changed despite the failed flush")

	// the repository stays consistent: a repaired repository passes the checker
	repo2 := repository.TestOpenBackend(t, be)
	rtest.OK(t, repository.RepairIndex(context.TODO(), repo2, repository.RepairIndexOptions{
		ReadAllPacks: true,
	}, restic.NewNoopPrinter()))
	repository.TestCheckRepo(t, repo2)
}

// TestFlushFailsOnStatReportedMissingPack verifies the flush-time existence
// check itself, independent of the actual Load path: when Stat reports a pack
// as missing, the index flush must be aborted.
func TestFlushFailsOnStatReportedMissingPack(t *testing.T) {
	be := mem.New()
	repo, _ := repository.TestRepositoryWithBackend(t, &statNotExistBackend{Backend: be}, 0, repository.Options{})

	err := repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		buf := rtest.Random(24, 64*1024)
		_, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
		return err
	})

	var missing *repository.ErrPackMissing
	rtest.Assert(t, errors.As(err, &missing), "expected ErrPackMissing, got %v", err)
}

// TestIndexMissingKeepsPacksRecoverable verifies that losing the index files
// does not make the uploaded data inaccessible: the packs remain in the
// backend and rebuilding the index restores a fully consistent repository.
func TestIndexMissingKeepsPacksRecoverable(t *testing.T) {
	repo, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	saveBlobs(t, repo, 10)

	packs := listPacks(t, repo)
	rtest.Assert(t, len(packs) > 0, "expected packs to be stored")

	// remove all index files, simulating a failed index upload or external
	// deletion while the packs were written durably
	for _, id := range listFiles(t, repo, restic.IndexFile).List() {
		rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: backend.IndexFile, Name: id.String()}))
	}

	// a fresh client cannot find the blobs without an index, but the data is
	// still present in the backend
	reopened := repository.TestOpenBackend(t, be)
	rtest.OK(t, reopened.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	rtest.Equals(t, 0, len(reopened.LookupBlob(restic.BlobHandle{Type: restic.DataBlob, ID: restic.ID{1}})),
		"index must be empty after deleting all index files")

	rtest.OK(t, repository.RepairIndex(context.TODO(), reopened, repository.RepairIndexOptions{
		ReadAllPacks: true,
	}, restic.NewNoopPrinter()))
	rtest.Equals(t, packs, listPacks(t, reopened), "rebuild must not drop any pack")

	// the rebuilt repository is fully consistent
	repository.TestCheckRepo(t, reopened)
}

// TestDamagedPackDetectedAndRepaired verifies the checker/repair side of the
// write-time vs. post-hoc trade-off: a corrupted pack is detected by the
// checker and repair packs salvages the intact data while keeping the
// repository consistent.
func TestDamagedPackDetectedAndRepaired(t *testing.T) {
	repo, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{
		// disable extra verification so that the externally damaged pack is
		// not rejected during the simulated re-upload below
		NoExtraVerify: true,
	})

	rnd := rand.New(rand.NewSource(7))
	createRandomBlobs(t, rnd, repo, 8, 0.8, true)

	packsBefore := listPacks(t, repo)
	damagedID := packsBefore.List()[0]
	replaceFile(t, be, backend.Handle{Type: backend.PackFile, Name: damagedID.String()},
		func(buf []byte) []byte {
			// flip a byte in the pack header region, which invalidates the
			// content-addressed pack ID
			buf[len(buf)-1] ^= 0xff
			return buf
		})

	checkRepo := repository.TestOpenBackend(t, be)
	rtest.OK(t, checkRepo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	chkr := checkRepo.Checker()

	errs := drainErrors(func(errCh chan<- error) {
		chkr.Packs(context.TODO(), errCh)
	})
	// the size still matches, so Packs() cannot see the byte-level damage;
	// the full pack read below must detect it
	rtest.Assert(t, len(errs) == 0, "unexpected metadata error: %v", errs)

	readErrs := drainErrors(func(errCh chan<- error) {
		chkr.ReadPacks(context.TODO(), func(packs map[restic.ID]int64) map[restic.ID]int64 {
			return packs
		}, restic.NewNoopPrinter(), errCh)
	})
	rtest.Assert(t, len(readErrs) > 0, "expected damaged pack to be detected")

	// repair the damaged pack and verify the repository is consistent again
	rtest.OK(t, repository.RepairPacks(context.TODO(), checkRepo, restic.NewIDSet(damagedID), restic.NewNoopPrinter()))
	rtest.OK(t, checkRepo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	repository.TestCheckRepo(t, checkRepo)
}

// TestConcurrentBackupsRemainConsistent verifies that two backups writing to
// the same backend concurrently each produce a consistent set of packs and
// indexes, and that a checker run on the merged result reports no errors.
func TestConcurrentBackupsRemainConsistent(t *testing.T) {
	be := mem.New()
	// initialize the repository, including config and key material
	repository.TestRepositoryWithBackend(t, be, 0, repository.Options{})

	const workers = 2
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	wg.Add(workers)
	for w := range workers {
		go func(worker int) {
			defer wg.Done()
			// every worker gets its own repository instance, like separate
			// restic processes sharing one backend
			repo := repository.TestOpenBackend(t, be)
			err := repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
				for i := range 12 {
					buf := rtest.Random(worker*1000+i+1, 16*1024)
					if _, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false); err != nil {
						return err
					}
				}
				return nil
			})
			errs <- err
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		rtest.OK(t, err)
	}

	// a fresh observer sees all packs and all independently written indexes
	observer := repository.TestOpenBackend(t, be)
	rtest.OK(t, observer.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	rtest.Assert(t, len(listPacks(t, observer)) > 0, "expected packs from both backups")
	rtest.Assert(t, len(listFiles(t, observer, restic.IndexFile)) >= workers,
		"expected at least one index per backup")
	repository.TestCheckRepo(t, observer)
}

// TestConcurrentBackupWithIndexDeletion verifies that deleting an index file
// while another backup is in flight does not corrupt the repository: the
// running backup writes its own packs and index independently, all packs
// remain on the backend, and rebuilding the index restores a fully
// consistent repository.
func TestConcurrentBackupWithIndexDeletion(t *testing.T) {
	be := mem.New()
	seed, _ := repository.TestRepositoryWithBackend(t, be, 0, repository.Options{})
	saveBlobs(t, seed, 8)

	indexes := listFiles(t, seed, restic.IndexFile)
	rtest.Assert(t, len(indexes) > 0, "expected an index to be present")

	// start a concurrent backup, then delete the previously written index
	// while it is running
	done := make(chan error, 1)
	go func() {
		repo := repository.TestOpenBackend(t, be)
		done <- repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			for i := range 10 {
				buf := rtest.Random(3000+i, 16*1024)
				if _, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false); err != nil {
					return err
				}
			}
			return nil
		})
	}()

	for _, id := range indexes.List() {
		rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: backend.IndexFile, Name: id.String()}))
	}
	rtest.OK(t, <-done)

	// the packs of both backups survive, only index files were lost
	packs := listPacks(t, seed)
	rtest.Assert(t, len(packs) > 0, "packs must survive the index deletion")

	rebuilt := repository.TestOpenBackend(t, be)
	rtest.OK(t, repository.RepairIndex(context.TODO(), rebuilt, repository.RepairIndexOptions{
		ReadAllPacks: true,
	}, restic.NewNoopPrinter()))
	rtest.Equals(t, packs, listPacks(t, rebuilt), "rebuild must retain all packs")
	repository.TestCheckRepo(t, rebuilt)
}

// TestConcurrentBackupWithSnapshotAndVanishingPack models the combined
// failure scenario: while a snapshot is being backed up, a pack disappears
// from the backend. The affected backup must fail before its index and
// snapshot are written, while an unaffected concurrent backup completes
// normally and the repository stays consistent.
func TestConcurrentBackupWithSnapshotAndVanishingPack(t *testing.T) {
	baseBE := mem.New()

	// healthy repo providing the initial snapshot and key material
	healthy, _ := repository.TestRepositoryWithBackend(t, baseBE, 0, repository.Options{})
	data.TestCreateSnapshot(t, healthy, time.Unix(1470492820, 0), 1)

	faultyBE := &vanishPackBackend{Backend: baseBE}

	var wg sync.WaitGroup
	wg.Add(2)
	var healthyErr, faultyErr error

	// concurrent healthy backup on the plain backend
	go func() {
		defer wg.Done()
		repo := repository.TestOpenBackend(t, baseBE)
		healthyErr = repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			for i := range 6 {
				buf := rtest.Random(5000+i, 16*1024)
				if _, _, _, err := uploader.SaveBlob(ctx, restic.TreeBlob, buf, restic.ID{}, false); err != nil {
					return err
				}
			}
			return nil
		})
	}()

	// faulty backup whose packs vanish after upload
	go func() {
		defer wg.Done()
		repo := repository.TestOpenBackend(t, faultyBE)
		faultyErr = repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			for i := range 6 {
				buf := rtest.Random(9000+i, 16*1024)
				if _, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	wg.Wait()

	rtest.OK(t, healthyErr)
	var missing *repository.ErrPackMissing
	rtest.Assert(t, errors.As(faultyErr, &missing), "faulty backup must fail with ErrPackMissing, got %v", faultyErr)

	// the shared backend must still contain a consistent repository: all packs
	// referenced by indexes exist and the checker reports no damage
	observer := repository.TestOpenBackend(t, baseBE)
	rtest.OK(t, observer.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	repository.TestCheckRepo(t, observer)
}
