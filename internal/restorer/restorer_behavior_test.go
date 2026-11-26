package restorer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/restic/restic/internal/repository"
	rtest "github.com/restic/restic/internal/test"
)

// TestRestoreDefaultOverwriteNever verifies that a restorer without an
// explicit overwrite option never clobbers existing files.
func TestRestoreDefaultOverwriteNever(t *testing.T) {
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "snapshot content\n"},
			"new": File{Data: "new file\n"},
		},
	}

	repo := repository.TestRepository(t)
	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)

	tempdir := filepath.Join(rtest.TempDir(t), "target")
	ctx := t.Context()

	// pre-existing file with different content
	existing := filepath.Join(tempdir, "foo")
	rtest.OK(t, os.MkdirAll(tempdir, 0700))
	rtest.OK(t, os.WriteFile(existing, []byte("existing content\n"), 0600))

	res := NewRestorer(repo, sn, Options{})
	_, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	// existing file must be left untouched, including its content
	got, err := os.ReadFile(existing)
	rtest.OK(t, err)
	rtest.Equals(t, "existing content\n", string(got))

	// files not present before must still be restored
	got, err = os.ReadFile(filepath.Join(tempdir, "new"))
	rtest.OK(t, err)
	rtest.Equals(t, "new file\n", string(got))

	rtest.Equals(t, 0, len(res.Failures()))
}

// TestRestoreOverwriteNeverSkipsMetadata makes sure that skipped files keep
// both their content and their permissions.
func TestRestoreOverwriteNeverSkipsMetadata(t *testing.T) {
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "snapshot content\n", Mode: 0o600},
		},
	}

	repo := repository.TestRepository(t)
	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)

	tempdir := filepath.Join(rtest.TempDir(t), "target")
	ctx := t.Context()
	existing := filepath.Join(tempdir, "foo")
	rtest.OK(t, os.MkdirAll(tempdir, 0700))
	rtest.OK(t, os.WriteFile(existing, []byte("existing\n"), 0o644))

	res := NewRestorer(repo, sn, Options{Overwrite: OverwriteNever})
	_, err := res.RestoreTo(ctx, tempdir)
	rtest.OK(t, err)

	fi, err := os.Stat(existing)
	rtest.OK(t, err)
	rtest.Equals(t, os.FileMode(0o644), fi.Mode().Perm(), "permissions of skipped file changed")
}

// TestRestoreCollectsFailures points the restorer at an unwritable location
// and verifies that individual item failures are reported and collected
// instead of aborting the whole restore.
func TestRestoreCollectsFailures(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read-only directory is not enforced for root")
	}

	snapshot := Snapshot{
		Nodes: map[string]Node{
			"subdir": Dir{
				Nodes: map[string]Node{
					"foo": File{Data: "content: foo\n"},
					"bar": File{Data: "content: bar\n"},
				},
			},
		},
	}

	repo := repository.TestRepository(t)
	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)

	tempdir := filepath.Join(rtest.TempDir(t), "target")
	rtest.OK(t, os.MkdirAll(tempdir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(tempdir, 0o700) })

	res := NewRestorer(repo, sn, Options{})
	var reported int
	res.Error = func(location string, err error) error {
		reported++
		return nil // continue on error
	}

	_, restoreErr := res.RestoreTo(t.Context(), tempdir)
	rtest.OK(t, restoreErr)

	failures := res.Failures()
	rtest.Assert(t, reported > 0, "expected the Error callback to be invoked")
	rtest.Equals(t, reported, len(failures), "every reported error should be collected")
	for _, failure := range failures {
		rtest.Assert(t, failure.Location != "", "failure location is empty")
		rtest.Assert(t, failure.Err != nil, "failure error is nil")
		rtest.Assert(t, failure.Path != "", "failure path is empty")
	}
}

// TestFileRestorerCleanupCreatedFiles checks the cleanup routine directly:
// files tracked as created by the restore are removed, while files that
// pre-existed are kept.
func TestFileRestorerCleanupCreatedFiles(t *testing.T) {
	repo := repository.TestRepository(t)
	dir := t.TempDir()
	fr := newFileRestorer(dir, nil, nil, 2, false, false, nil, nil, repo.ChunkerFactory().ZeroChunk())
	fr.cleanupOnCancel = true

	created := filepath.Join(dir, "created")
	existing := filepath.Join(dir, "existing")

	// mirror the real ordering: trackFileCreation runs at create time,
	// before createFile opens the path
	fr.trackFileCreation(created) // path does not exist yet -> tracked as created
	rtest.OK(t, os.WriteFile(created, []byte("partial"), 0600))

	rtest.OK(t, os.WriteFile(existing, []byte("keep"), 0600))
	fr.trackFileCreation(existing) // path already exists -> marked pre-existing

	fr.cleanupCreatedFiles()

	_, err := os.Lstat(created)
	rtest.Assert(t, os.IsNotExist(err), "created file should have been removed: %v", err)
	got, err := os.ReadFile(existing)
	rtest.OK(t, err)
	rtest.Equals(t, "keep", string(got))
}

// TestRestoreCancelReturnsError verifies that a context canceled before the
// restore starts makes RestoreTo return an error rather than silently
// succeeding.
func TestRestoreCancelReturnsError(t *testing.T) {
	repo := repository.TestRepository(t)
	snapshot := Snapshot{
		Nodes: map[string]Node{
			"foo": File{Data: "content: foo\n"},
		},
	}
	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)

	tempdir := filepath.Join(rtest.TempDir(t), "target")
	rtest.OK(t, os.MkdirAll(tempdir, 0o700))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := NewRestorer(repo, sn, Options{CleanupOnCancel: true})
	res.Error = func(string, error) error { return nil }
	_, err := res.RestoreTo(ctx, tempdir)
	rtest.Assert(t, err != nil, "expected restore to fail after cancellation")
}

// TestRestoreWorkerCountIsHonored checks that the worker count reaches the
// underlying file restorer.
func TestRestoreWorkerCountIsHonored(t *testing.T) {
	repo := repository.TestRepository(t)
	sn, _ := saveSnapshot(t, repo, Snapshot{
		Nodes: map[string]Node{"foo": File{Data: "x"}},
	}, noopGetGenericAttributes)

	res := NewRestorer(repo, sn, Options{WorkerCount: 3})
	rtest.Equals(t, 3, res.workerCount())

	// zero falls back to repository connections
	res = NewRestorer(repo, sn, Options{})
	rtest.Assert(t, res.workerCount() >= 1, "worker count must be positive")
}

// TestRestoreFailuresResetBetweenRuns makes sure failures collected by one
// RestoreTo call do not leak into a subsequent successful call.
func TestRestoreFailuresResetBetweenRuns(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read-only directory is not enforced for root")
	}

	snapshot := Snapshot{
		Nodes: map[string]Node{
			"subdir": Dir{
				Nodes: map[string]Node{
					"foo": File{Data: "content: foo\n"},
				},
			},
		},
	}
	repo := repository.TestRepository(t)
	sn, _ := saveSnapshot(t, repo, snapshot, noopGetGenericAttributes)

	res := NewRestorer(repo, sn, Options{})
	res.Error = func(string, error) error { return nil }

	// first run into a read-only target collects failures
	ro := filepath.Join(rtest.TempDir(t), "readonly")
	rtest.OK(t, os.MkdirAll(ro, 0o500))
	t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
	_, err := res.RestoreTo(t.Context(), ro)
	rtest.OK(t, err)
	rtest.Assert(t, len(res.Failures()) > 0, "expected failures from read-only target")

	// second run into a writable target must start with an empty list
	ok := filepath.Join(rtest.TempDir(t), "writable")
	rtest.OK(t, os.MkdirAll(ok, 0o700))
	_, err = res.RestoreTo(t.Context(), ok)
	rtest.OK(t, err)
	rtest.Equals(t, 0, len(res.Failures()), "failures from the previous run leaked")

	got, err := os.ReadFile(filepath.Join(ok, "subdir", "foo"))
	rtest.OK(t, err)
	rtest.Equals(t, "content: foo\n", string(got))
}
