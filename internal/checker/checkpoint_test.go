package checker_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func paddedID(prefix, fill string) restic.ID {
	return restic.TestParseID(prefix + strings.Repeat(fill, 64-len(prefix)))
}

func TestParsePackRange(t *testing.T) {
	full := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	valid := []string{"00..ff", "0123..0124", full, "deadbeef-deadbef0", "0123:0124", "0123...0124"}
	for _, in := range valid {
		_, err := checker.ParsePackRange(in)
		rtest.OK(t, err)
	}

	invalid := []string{"", "deadbf..deadbe", "zz..ff", "--", "01-00"}
	for _, in := range invalid {
		_, err := checker.ParsePackRange(in)
		rtest.Assert(t, err != nil, "expected error for %q", in)
	}

	r, err := checker.ParsePackRange("01..02")
	rtest.OK(t, err)
	rtest.Assert(t, r.Contains(paddedID("01", "0")), "expected lower boundary included")
	rtest.Assert(t, r.Contains(paddedID("02", "f")), "expected upper prefix boundary included")
	rtest.Assert(t, !r.Contains(paddedID("03", "0")), "expected ID above range excluded")
	rtest.Assert(t, !r.Contains(paddedID("00", "f")), "expected ID below range excluded")

	exact, err := checker.ParsePackRange("ab")
	rtest.OK(t, err)
	rtest.Assert(t, exact.Contains(paddedID("ab", "0")), "prefix range should include first prefix ID")
	rtest.Assert(t, exact.Contains(paddedID("ab", "f")), "prefix range should include last prefix ID")
	rtest.Assert(t, !exact.Contains(paddedID("ac", "0")), "prefix range should exclude next prefix")

	fullID := paddedID("ab", "0").String()
	exactFull, err := checker.ParsePackRange(fullID)
	rtest.OK(t, err)
	rtest.Assert(t, exactFull.Contains(restic.TestParseID(fullID)), "full ID should select itself")
	rtest.Assert(t, !exactFull.Contains(paddedID("ac", "0")), "lone full ID should be exact")
}

func TestSelectPacksByRange(t *testing.T) {
	inRange := paddedID("11", "0")
	outRange := paddedID("99", "0")
	packs := map[restic.ID]int64{inRange: 10, outRange: 20}

	r, err := checker.ParsePackRange("10..20")
	rtest.OK(t, err)
	selected := checker.SelectPacksByRange(packs, r)
	rtest.Equals(t, 1, len(selected))
	_, ok := selected[inRange]
	rtest.Assert(t, ok, "expected in-range pack")
	_, ok = selected[outRange]
	rtest.Assert(t, !ok, "out-of-range pack must not participate")
}

func TestCheckpointResumeAndScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	id := paddedID("ab", "0")

	cp, err := checker.OpenCheckpoint(path, "repo-id", "scope-a")
	rtest.OK(t, err)
	rtest.Assert(t, !cp.IsVerified(id), "new checkpoint should not contain pack")
	rtest.OK(t, cp.Record(checker.NewPackResult(id, 123, nil)))
	rtest.Assert(t, cp.IsVerified(id), "recorded successful pack should be verified")
	rtest.OK(t, cp.Close())

	resumed, err := checker.OpenCheckpoint(path, "repo-id", "scope-a")
	rtest.OK(t, err)
	rtest.Assert(t, resumed.IsVerified(id), "verified result must survive restart")
	verified, damaged, failed := resumed.Counts()
	rtest.Equals(t, 1, verified)
	rtest.Equals(t, 0, damaged)
	rtest.Equals(t, 0, failed)
	rtest.OK(t, resumed.Close())

	_, err = checker.OpenCheckpoint(path, "repo-id", "scope-b")
	rtest.Assert(t, err != nil, "different scope must not silently resume")
	_, err = checker.OpenCheckpoint(path, "other-repo", "scope-a")
	rtest.Assert(t, err != nil, "different repository must not silently resume")
}

func TestCheckpointLatestFailureAndReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	id := paddedID("cd", "0")
	cp, err := checker.OpenCheckpoint(path, "repo-id", "scope")
	rtest.OK(t, err)

	packErr := &repository.ErrPackData{PackID: id}
	rtest.OK(t, cp.Record(checker.NewPackResult(id, 10, packErr)))
	rtest.Assert(t, !cp.IsVerified(id), "damaged pack must not be skipped as verified")
	damaged, failed := 0, 0
	for _, result := range cp.Results() {
		switch result.Status {
		case checker.PackDamaged:
			damaged++
		case checker.PackFailed:
			failed++
		}
	}
	rtest.Equals(t, 1, damaged)
	rtest.Equals(t, 0, failed)

	rtest.OK(t, cp.Record(checker.NewPackResult(id, 10, nil)))
	rtest.Assert(t, cp.IsVerified(id), "latest successful result should replace failure")
	rtest.OK(t, cp.Close())
}
