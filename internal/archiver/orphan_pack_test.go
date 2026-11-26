package archiver

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// TestSnapshotCleansOrphanPacks verifies that starting a backup removes pack
// files left behind by an interrupted previous backup, while still completing
// the new backup successfully.
func TestSnapshotCleansOrphanPacks(t *testing.T) {
	files := TestDir{
		"testfile": TestFile{
			Content: "back up this file",
		},
	}
	tempdir := rtest.TempDir(t)
	TestCreateFiles(t, tempdir, files)

	be := mem.New()
	repo, _ := repository.TestRepositoryWithBackend(t, be, 0, repository.Options{})

	// plant an orphaned pack, as if a previous backup had uploaded it but
	// crashed before flushing the index
	orphan := restic.NewRandomID()
	h := backend.Handle{Type: backend.PackFile, Name: orphan.String()}
	rtest.OK(t, be.Save(context.TODO(), h, backend.NewByteReader([]byte("leftover pack data"), be.Hasher())))

	// the index must be loaded so that CleanupOrphanPacks can compare the
	// backend packs against the indexed packs
	rtest.OK(t, repo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))

	TestSnapshot(t, repo, filepath.Join(tempdir, "testfile"), nil)

	// the orphaned pack is gone
	_, err := be.Stat(context.TODO(), h)
	rtest.Assert(t, be.IsNotExist(err), "orphaned pack should be removed by backup, got err %v", err)

	// a repository check must not report any problems
	chkr := repo.Checker()
	errChan := make(chan error)
	go chkr.Packs(context.TODO(), errChan)
	for err := range errChan {
		t.Errorf("checker reported pack error: %v", err)
	}
}
