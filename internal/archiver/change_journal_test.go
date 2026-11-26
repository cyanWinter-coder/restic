package archiver

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/restic/restic/internal/restic"
)

func TestMemoryChangeJournalAssociatesOnlyPendingRecords(t *testing.T) {
	ctx := context.Background()
	j := NewMemoryChangeJournal()

	first, err := j.RecordReadChange(ctx, ReadChange{Path: "a", DetectedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	second, err := j.RecordReadChange(ctx, ReadChange{Path: "b", DetectedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}

	pending, err := j.PendingReadChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d, want 2", len(pending))
	}

	nextID := restic.NewRandomID()
	if err := j.AssociateReadChanges(ctx, nextID, []ReadChange{first}); err != nil {
		t.Fatal(err)
	}

	pending, err = j.PendingReadChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Sequence != second.Sequence {
		t.Fatalf("pending after association = %+v, want only sequence %d", pending, second.Sequence)
	}

	all := j.changes
	if all[0].NextSnapshot == nil || !all[0].NextSnapshot.Equal(nextID) {
		t.Fatalf("first change not associated with %v: %+v", nextID, all[0])
	}
	if all[1].NextSnapshot != nil {
		t.Fatal("second change was associated unexpectedly")
	}
}

func TestFileChangeJournalPersistsAssociation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state", "read-changes.json")
	j := NewFileChangeJournal(path)

	change, err := j.RecordReadChange(ctx, ReadChange{
		Path:       "changing-file",
		Before:     FileState{Size: 1, ModTime: time.Unix(100, 0)},
		After:      FileState{Size: 2, ModTime: time.Unix(200, 0)},
		DetectedAt: time.Unix(300, 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Reopen from disk to ensure the record survived.
	reopened := NewFileChangeJournal(path)
	pending, err := reopened.PendingReadChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Path != "changing-file" || pending[0].Sequence != change.Sequence {
		t.Fatalf("unexpected persisted pending changes: %+v", pending)
	}

	nextID := restic.NewRandomID()
	if err := reopened.AssociateReadChanges(ctx, nextID, pending); err != nil {
		t.Fatal(err)
	}

	pending, err = NewFileChangeJournal(path).PendingReadChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("associated change still pending: %+v", pending)
	}
}
