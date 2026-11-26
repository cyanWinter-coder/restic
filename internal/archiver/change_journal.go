package archiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
)

// FileState is the size and modification time observed for a file.
type FileState struct {
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

func fileStateFromFI(fi *fs.ExtendedFileInfo) FileState {
	if fi == nil {
		return FileState{}
	}
	return FileState{Size: fi.Size, ModTime: fi.ModTime}
}

// ReadChange describes a file whose size or modification time changed while
// it was being read. A zero After with Deleted set means that the path
// disappeared after its contents had been read.
type ReadChange struct {
	// Sequence is assigned by a ChangeJournal implementation.
	Sequence uint64 `json:"sequence,omitempty"`

	Path   string    `json:"path"`
	Before FileState `json:"before"`
	After  FileState `json:"after,omitempty"`

	Deleted   bool   `json:"deleted,omitempty"`
	StatError string `json:"stat_error,omitempty"`

	DetectedAt time.Time `json:"detected_at"`

	// DetectedInSnapshot is the snapshot produced by the backup during which
	// the change was observed, when that ID is known.
	DetectedInSnapshot *restic.ID `json:"detected_in_snapshot,omitempty"`

	// NextSnapshot is the first later snapshot that consumed/associated this
	// pending change.
	NextSnapshot *restic.ID `json:"next_snapshot,omitempty"`
}

// Changed reports whether the before and after states differ in size or mtime.
func (c ReadChange) Changed() bool {
	return c.Deleted || c.StatError != "" || c.Before.Size != c.After.Size || !c.Before.ModTime.Equal(c.After.ModTime)
}

// ReadChangeError is reported through Archiver.Error for a file that changed
// during reading. It does not by itself abort the backup.
type ReadChangeError struct {
	Change ReadChange
}

func (e *ReadChangeError) Error() string {
	switch {
	case e.Change.Deleted:
		return fmt.Sprintf("%v was deleted while it was being read; recorded for the next snapshot", e.Change.Path)
	case e.Change.StatError != "":
		return fmt.Sprintf("%v could not be rechecked after reading: %v", e.Change.Path, e.Change.StatError)
	default:
		return fmt.Sprintf("%v changed while it was being read (size %d→%d, mtime %s→%s); recorded for the next snapshot",
			e.Change.Path,
			e.Change.Before.Size, e.Change.After.Size,
			e.Change.Before.ModTime.Format(time.RFC3339Nano),
			e.Change.After.ModTime.Format(time.RFC3339Nano))
	}
}

// ChangeJournal stores read-consistency observations. Pending observations
// are associated with the next successfully created snapshot.
type ChangeJournal interface {
	RecordReadChange(ctx context.Context, change ReadChange) (ReadChange, error)
	PendingReadChanges(ctx context.Context) ([]ReadChange, error)
	AssociateReadChanges(ctx context.Context, snapshotID restic.ID, changes []ReadChange) error
}

// detectedSnapshotJournal is an optional journal capability that records which
// snapshot observed a change. Pending changes are still reserved for the next
// snapshot.
type detectedSnapshotJournal interface {
	TagDetectedSnapshot(ctx context.Context, snapshotID restic.ID, notBefore time.Time) error
}

// MemoryChangeJournal is a concurrency-safe in-memory ChangeJournal.
type MemoryChangeJournal struct {
	mu      sync.Mutex
	nextSeq uint64
	changes []ReadChange
}

// NewMemoryChangeJournal returns an empty in-memory change journal.
func NewMemoryChangeJournal() *MemoryChangeJournal {
	return &MemoryChangeJournal{}
}

// RecordReadChange appends a change and assigns it a sequence number.
func (j *MemoryChangeJournal) RecordReadChange(_ context.Context, change ReadChange) (ReadChange, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.nextSeq++
	change.Sequence = j.nextSeq
	j.changes = append(j.changes, change)
	return change, nil
}

// PendingReadChanges returns changes that have not yet been associated with a
// later snapshot.
func (j *MemoryChangeJournal) PendingReadChanges(_ context.Context) ([]ReadChange, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.pendingLocked(), nil
}

func (j *MemoryChangeJournal) pendingLocked() []ReadChange {
	var out []ReadChange
	for _, change := range j.changes {
		if change.NextSnapshot == nil {
			out = append(out, change)
		}
	}
	return cloneReadChanges(out)
}

// TagDetectedSnapshot tags pending records detected after notBefore.
func (j *MemoryChangeJournal) TagDetectedSnapshot(_ context.Context, snapshotID restic.ID, notBefore time.Time) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for i := range j.changes {
		if j.changes[i].NextSnapshot != nil || j.changes[i].DetectedAt.Before(notBefore) {
			continue
		}
		id := snapshotID
		j.changes[i].DetectedInSnapshot = &id
	}
	return nil
}

// AssociateReadChanges marks the supplied changes by sequence number as
// consumed by snapshotID.
func (j *MemoryChangeJournal) AssociateReadChanges(_ context.Context, snapshotID restic.ID, changes []ReadChange) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	ids := make(map[uint64]struct{}, len(changes))
	for _, change := range changes {
		if change.Sequence != 0 {
			ids[change.Sequence] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return nil
	}

	for i := range j.changes {
		if _, ok := ids[j.changes[i].Sequence]; ok && j.changes[i].NextSnapshot == nil {
			id := snapshotID
			j.changes[i].NextSnapshot = &id
		}
	}
	return nil
}

func cloneReadChanges(in []ReadChange) []ReadChange {
	if in == nil {
		return nil
	}
	out := make([]ReadChange, len(in))
	copy(out, in)
	return out
}

type fileChangeJournalData struct {
	NextSequence uint64       `json:"next_sequence"`
	Changes      []ReadChange `json:"changes"`
}

// FileChangeJournal persists changes as JSON so the pending association can
// survive across backup invocations.
type FileChangeJournal struct {
	path string
	mu   sync.Mutex
}

// NewFileChangeJournal creates or opens the journal at path.
func NewFileChangeJournal(path string) *FileChangeJournal {
	return &FileChangeJournal{path: path}
}

// Path returns the journal file path.
func (j *FileChangeJournal) Path() string { return j.path }

func (j *FileChangeJournal) loadLocked() (fileChangeJournalData, error) {
	data := fileChangeJournalData{}
	b, err := os.ReadFile(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return data, nil
	}
	if err != nil {
		return data, fmt.Errorf("read change journal %v: %w", j.path, err)
	}
	if len(b) == 0 {
		return data, nil
	}
	if err := json.Unmarshal(b, &data); err != nil {
		return data, fmt.Errorf("parse change journal %v: %w", j.path, err)
	}
	return data, nil
}

func (j *FileChangeJournal) saveLocked(data fileChangeJournalData) error {
	if err := os.MkdirAll(filepath.Dir(j.path), 0o700); err != nil {
		return fmt.Errorf("create change journal directory: %w", err)
	}

	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(j.path), ".change-journal-*")
	if err != nil {
		return fmt.Errorf("create change journal temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write change journal: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod change journal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close change journal: %w", err)
	}
	if err := os.Rename(tmpName, j.path); err != nil {
		return fmt.Errorf("replace change journal: %w", err)
	}
	return nil
}

// RecordReadChange persists a new pending change.
func (j *FileChangeJournal) RecordReadChange(_ context.Context, change ReadChange) (ReadChange, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := j.loadLocked()
	if err != nil {
		return ReadChange{}, err
	}
	data.NextSequence++
	change.Sequence = data.NextSequence
	data.Changes = append(data.Changes, change)
	if err := j.saveLocked(data); err != nil {
		return ReadChange{}, err
	}
	return change, nil
}

// PendingReadChanges returns all unassociated persisted changes.
func (j *FileChangeJournal) PendingReadChanges(_ context.Context) ([]ReadChange, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := j.loadLocked()
	if err != nil {
		return nil, err
	}
	var out []ReadChange
	for _, change := range data.Changes {
		if change.NextSnapshot == nil {
			out = append(out, change)
		}
	}
	return out, nil
}

// AssociateReadChanges marks matching persisted pending changes with snapshotID.
func (j *FileChangeJournal) AssociateReadChanges(_ context.Context, snapshotID restic.ID, changes []ReadChange) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := j.loadLocked()
	if err != nil {
		return err
	}
	ids := make(map[uint64]struct{}, len(changes))
	for _, change := range changes {
		if change.Sequence != 0 {
			ids[change.Sequence] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return nil
	}

	changed := false
	for i := range data.Changes {
		if _, ok := ids[data.Changes[i].Sequence]; ok && data.Changes[i].NextSnapshot == nil {
			id := snapshotID
			data.Changes[i].NextSnapshot = &id
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return j.saveLocked(data)
}

// TagDetectedSnapshot tags pending records detected after notBefore.
func (j *FileChangeJournal) TagDetectedSnapshot(_ context.Context, snapshotID restic.ID, notBefore time.Time) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := j.loadLocked()
	if err != nil {
		return err
	}

	changed := false
	for i := range data.Changes {
		if data.Changes[i].NextSnapshot != nil || data.Changes[i].DetectedAt.Before(notBefore) {
			continue
		}
		id := snapshotID
		data.Changes[i].DetectedInSnapshot = &id
		changed = true
	}
	if !changed {
		return nil
	}
	return j.saveLocked(data)
}
