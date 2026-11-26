package checker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

// PackRange is an inclusive, lexicographical range of pack IDs.
//
// The zero value matches every pack. A range with only one boundary is
// unbounded on the other side.
type PackRange struct {
	Start restic.ID
	End   restic.ID
	// HasStart and HasEnd distinguish an explicit zero ID from an unset
	// boundary.
	HasStart bool
	HasEnd   bool
}

// ParsePackRange parses an inclusive pack ID range.
//
// Supported forms are:
//
//	START..END
//	START...END
//	START:END
//	START-END
//
// Either boundary may be omitted. A lone full ID selects that pack; a lone
// abbreviated prefix selects the whole ID-prefix interval. Boundaries may be
// abbreviated hexadecimal prefixes: a start prefix is padded with zeroes and an
// end prefix is padded with f's.
func ParsePackRange(s string) (PackRange, error) {
	original := strings.TrimSpace(s)
	if original == "" {
		return PackRange{}, errors.New("empty pack ID range")
	}

	var startText, endText string
	switch {
	case strings.Contains(original, "..."):
		parts := strings.SplitN(original, "...", 2)
		startText, endText = parts[0], parts[1]
	case strings.Contains(original, ".."):
		parts := strings.SplitN(original, "..", 2)
		startText, endText = parts[0], parts[1]
	case strings.Contains(original, ":"):
		parts := strings.SplitN(original, ":", 2)
		startText, endText = parts[0], parts[1]
	case strings.Contains(original, "-"):
		parts := strings.SplitN(original, "-", 2)
		startText, endText = parts[0], parts[1]
	default:
		// A single token selects an exact full ID or the complete prefix
		// range when abbreviated.
		startText = original
		endText = original
	}

	r := PackRange{}
	var err error
	if strings.TrimSpace(startText) != "" {
		r.Start, err = parsePackIDBoundary(startText, false)
		if err != nil {
			return PackRange{}, errors.Wrap(err, "invalid range start")
		}
		r.HasStart = true
	}
	if strings.TrimSpace(endText) != "" {
		r.End, err = parsePackIDBoundary(endText, true)
		if err != nil {
			return PackRange{}, errors.Wrap(err, "invalid range end")
		}
		r.HasEnd = true
	}

	if r.HasStart && r.HasEnd && bytes.Compare(r.Start[:], r.End[:]) > 0 {
		return PackRange{}, errors.Errorf("pack range start %s is after end %s", r.Start, r.End)
	}
	return r, nil
}

func parsePackIDBoundary(s string, end bool) (restic.ID, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 || len(s) > len(restic.ID{})*2 {
		return restic.ID{}, errors.Errorf("invalid pack ID %q: expected 1 to 64 hex characters", s)
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return restic.ID{}, errors.Errorf("invalid pack ID %q: must contain hexadecimal characters only", s)
		}
	}
	if len(s) == len(restic.ID{})*2 {
		return restic.ParseID(strings.ToLower(s))
	}

	padded := s
	if end {
		padded += strings.Repeat("f", len(restic.ID{})*2-len(s))
	} else {
		padded += strings.Repeat("0", len(restic.ID{})*2-len(s))
	}
	return restic.ParseID(strings.ToLower(padded))
}

// Contains reports whether id is inside the inclusive range.
func (r PackRange) Contains(id restic.ID) bool {
	if r.HasStart && bytes.Compare(id[:], r.Start[:]) < 0 {
		return false
	}
	if r.HasEnd && bytes.Compare(id[:], r.End[:]) > 0 {
		return false
	}
	return true
}

// String returns a canonical representation of the range.
func (r PackRange) String() string {
	start, end := "", ""
	if r.HasStart {
		start = r.Start.String()
	}
	if r.HasEnd {
		end = r.End.String()
	}
	return start + ".." + end
}

// SelectPacksByRange returns the packs whose IDs are in r.
func SelectPacksByRange(packs map[restic.ID]int64, r PackRange) map[restic.ID]int64 {
	result := make(map[restic.ID]int64)
	for id, size := range packs {
		if r.Contains(id) {
			result[id] = size
		}
	}
	return result
}

// SortedPackIDs returns the IDs in lexicographical order. Map iteration must
// never be used to produce user-visible lists because it is randomized.
func SortedPackIDs(ids []restic.ID) []restic.ID {
	sort.Slice(ids, func(i, j int) bool {
		return bytes.Compare(ids[i][:], ids[j][:]) < 0
	})
	return ids
}

// PackStatus is the persisted result of checking one pack.
type PackStatus string

const (
	// PackVerified means the complete pack was read and verified.
	PackVerified PackStatus = "verified"
	// PackDamaged means a deterministic pack-integrity error was found.
	PackDamaged PackStatus = "damaged"
	// PackFailed means the pack could not be checked for another reason.
	PackFailed PackStatus = "failed"
)

// PackResult is one append-only checkpoint record.
type PackResult struct {
	Type      string     `json:"type"`
	ID        string     `json:"id"`
	Status    PackStatus `json:"status"`
	Size      int64      `json:"size,omitempty"`
	Error     string     `json:"error,omitempty"`
	CheckedAt time.Time  `json:"checked_at"`
}

// checkpointHeader identifies the repository and selection that a checkpoint
// belongs to. It prevents a resumed run from silently mixing different scopes.
type checkpointHeader struct {
	Type      string    `json:"type"`
	Version   int       `json:"version"`
	RepoID    string    `json:"repo_id"`
	Scope     string    `json:"scope"`
	CreatedAt time.Time `json:"created_at"`
}

// Checkpoint is an append-only, resumable record of checked packs.
//
// A checkpoint is safe for concurrent use. Each new result is flushed to the
// underlying file before Record returns. The latest result for a pack wins.
type Checkpoint struct {
	path    string
	header  checkpointHeader
	mu      sync.Mutex
	file    *os.File
	results map[restic.ID]PackResult
}

const checkpointVersion = 1

// OpenCheckpoint opens or creates a checkpoint at path. An existing checkpoint
// must belong to the same repository and scope.
func OpenCheckpoint(path, repoID, scope string) (*Checkpoint, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("checkpoint path is empty")
	}

	c := &Checkpoint{
		path: path,
		header: checkpointHeader{
			Type:      "restic-check-checkpoint",
			Version:   checkpointVersion,
			RepoID:    repoID,
			Scope:     scope,
			CreatedAt: time.Now().UTC(),
		},
		results: make(map[restic.ID]PackResult),
	}

	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := c.load(data); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, errors.Wrap(err, "open checkpoint for append")
		}
		c.file = f
	case errors.Is(err, os.ErrNotExist):
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, errors.Wrap(err, "create checkpoint")
		}
		c.file = f
		if err := c.writeHeader(); err != nil {
			_ = f.Close()
			return nil, err
		}
	default:
		return nil, errors.Wrap(err, "read checkpoint")
	}

	return c, nil
}

func (c *Checkpoint) load(data []byte) error {
	lines := bytes.Split(data, []byte("\n"))
	sawHeader := false
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var head struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &head); err != nil {
			// A crash while appending can leave only the final line torn.
			// Ignore that line; the corresponding pack is checked again.
			lastNonEmpty := i == len(lines)-1 || (i == len(lines)-2 && len(lines[len(lines)-1]) == 0)
			fileEndsWithNewline := len(data) > 0 && data[len(data)-1] == '\n'
			if lastNonEmpty && !fileEndsWithNewline {
				break
			}
			return errors.Wrapf(err, "parse checkpoint line %d", i+1)
		}

		if head.Type == c.header.Type {
			var header checkpointHeader
			if err := json.Unmarshal(line, &header); err != nil {
				return errors.Wrap(err, "parse checkpoint header")
			}
			if header.Version != checkpointVersion {
				return errors.Errorf("unsupported checkpoint version %d", header.Version)
			}
			if header.RepoID != c.header.RepoID {
				return errors.New("checkpoint belongs to a different repository")
			}
			if header.Scope != c.header.Scope {
				return errors.New("checkpoint scope does not match; use a new checkpoint file for this selection")
			}
			c.header = header
			sawHeader = true
			continue
		}

		var record PackResult
		if err := json.Unmarshal(line, &record); err != nil {
			return errors.Wrapf(err, "parse checkpoint record line %d", i+1)
		}
		id, err := restic.ParseID(record.ID)
		if err != nil {
			return errors.Wrapf(err, "invalid pack ID in checkpoint line %d", i+1)
		}
		c.results[id] = record
	}

	if !sawHeader {
		return errors.New("checkpoint is missing its header")
	}
	return nil
}

func (c *Checkpoint) writeHeader() error {
	line, err := json.Marshal(c.header)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := c.file.Write(line); err != nil {
		return errors.Wrap(err, "write checkpoint header")
	}
	return nil
}

// Record persists the latest result for result.ID.
func (c *Checkpoint) Record(result PackResult) error {
	id, err := restic.ParseID(result.ID)
	if err != nil {
		return errors.Wrap(err, "record checkpoint result")
	}
	if result.Type == "" {
		result.Type = "pack-result"
	}
	if result.CheckedAt.IsZero() {
		result.CheckedAt = time.Now().UTC()
	}

	line, err := json.Marshal(result)
	if err != nil {
		return errors.Wrap(err, "encode checkpoint result")
	}
	line = append(line, '\n')

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.file.Write(line); err != nil {
		return errors.Wrap(err, "append checkpoint result")
	}
	c.results[id] = result
	return nil
}

// IsVerified reports whether the latest persisted result for id is verified.
func (c *Checkpoint) IsVerified(id restic.ID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.results[id]
	return ok && r.Status == PackVerified
}

// IsCompleted reports whether the checkpoint has any terminal result for id.
func (c *Checkpoint) IsCompleted(id restic.ID) bool {
	_, ok := c.Result(id)
	return ok
}

// Result returns the latest persisted result for id.
func (c *Checkpoint) Result(id restic.ID) (PackResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.results[id]
	return r, ok
}

// Results returns all latest pack results sorted by pack ID.
func (c *Checkpoint) Results() []PackResult {
	c.mu.Lock()
	results := make([]PackResult, 0, len(c.results))
	for _, r := range c.results {
		results = append(results, r)
	}
	c.mu.Unlock()

	sort.Slice(results, func(i, j int) bool { return results[i].ID < results[j].ID })
	return results
}

// Counts returns the number of latest results by status.
func (c *Checkpoint) Counts() (verified, damaged, failed int) {
	for _, r := range c.Results() {
		switch r.Status {
		case PackVerified:
			verified++
		case PackDamaged:
			damaged++
		case PackFailed:
			failed++
		}
	}
	return verified, damaged, failed
}

// Close flushes and closes the checkpoint.
func (c *Checkpoint) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	return err
}

// NewPackResult classifies err and creates a checkpoint result.
func NewPackResult(id restic.ID, size int64, err error) PackResult {
	result := PackResult{
		Type:      "pack-result",
		ID:        id.String(),
		Size:      size,
		CheckedAt: time.Now().UTC(),
	}
	if err == nil {
		result.Status = PackVerified
		return result
	}

	var packDataErr *repository.ErrPackData
	if errors.As(err, &packDataErr) {
		result.Status = PackDamaged
	} else {
		result.Status = PackFailed
	}
	result.Error = err.Error()
	return result
}

// WritePackReport writes a stable, human-readable list of pack results.
func WritePackReport(w io.Writer, repoID, scope string, results []PackResult) error {
	writer := bufio.NewWriter(w)
	if _, err := fmt.Fprintf(writer, "restic check pack report\nrepository id: %s\nscope: %s\ngenerated at: %s\n\n",
		repoID, scope, time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}

	verified, damaged, failed := 0, 0, 0
	for _, r := range results {
		switch r.Status {
		case PackVerified:
			verified++
		case PackDamaged:
			damaged++
		case PackFailed:
			failed++
		}
	}
	if _, err := fmt.Fprintf(writer, "packs: %d total, %d verified, %d damaged, %d failed\n\n",
		len(results), verified, damaged, failed); err != nil {
		return err
	}

	for _, r := range results {
		if r.Status == PackVerified {
			continue
		}
		if _, err := fmt.Fprintf(writer, "%s\t%s", r.ID, r.Status); err != nil {
			return err
		}
		if r.Error != "" {
			if _, err := fmt.Fprintf(writer, "\t%s", strings.ReplaceAll(r.Error, "\n", " ")); err != nil {
				return err
			}
		}
		if err := writer.WriteByte('\n'); err != nil {
			return err
		}
	}
	return writer.Flush()
}
