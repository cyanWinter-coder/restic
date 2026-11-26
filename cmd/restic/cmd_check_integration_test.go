package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/global"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

func testRunCheck(t testing.TB, gopts global.Options) {
	t.Helper()
	stdout, stderr, err := testRunCheckOutput(t, gopts, true)
	if err != nil {
		t.Error(stdout)
		t.Error(stderr)
		t.Fatalf("unexpected error: %+v", err)
	}
}

func testRunCheckMustFail(t testing.TB, gopts global.Options) {
	t.Helper()
	_, _, err := testRunCheckOutput(t, gopts, false)
	rtest.Assert(t, err != nil, "expected non nil error after check of damaged repository")
}

func testRunCheckOutput(t testing.TB, gopts global.Options, checkUnused bool) (string, string, error) {
	stdout, stderr, err := withCaptureStdoutStderr(t, gopts, func(ctx context.Context, gopts global.Options) error {
		opts := CheckOptions{
			ReadData:    true,
			CheckUnused: checkUnused,
		}
		_, err := runCheck(ctx, opts, gopts, nil, gopts.Term)
		return err
	})
	return stdout.String(), stderr.String(), err
}

func testRunCheckOutputWithOpts(t testing.TB, gopts global.Options, opts CheckOptions, args []string) (string, error) {
	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.Verbosity = 2
		_, err := runCheck(ctx, opts, gopts, args, gopts.Term)
		return err
	})
	return buf.String(), err
}

func TestCheckWithSnaphotFilter(t *testing.T) {
	testCases := []struct {
		opts           CheckOptions
		args           []string
		expectedOutput string
	}{
		{ // full --read-data, all snapshots
			CheckOptions{ReadData: true},
			nil,
			"4 / 4 packs",
		},
		{ // full --read-data, all snapshots
			CheckOptions{ReadData: true},
			nil,
			"2 / 2 snapshots",
		},
		{ // full --read-data, latest snapshot
			CheckOptions{ReadData: true},
			[]string{"latest"},
			"2 / 2 packs",
		},
		{ // full --read-data, latest snapshot
			CheckOptions{ReadData: true},
			[]string{"latest"},
			"1 / 1 snapshots",
		},
		{ // --read-data-subset, latest snapshot
			CheckOptions{ReadDataSubset: "1%"},
			[]string{"latest"},
			"1 / 1 packs",
		},
		{ // --read-data-subset, latest snapshot
			CheckOptions{ReadDataSubset: "1%"},
			[]string{"latest"},
			"filtered",
		},
	}

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	opts := BackupOptions{}
	testRunBackup(t, env.testdata+"/0", []string{"for_cmd_ls"}, opts, env.gopts)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, opts, env.gopts)

	for _, testCase := range testCases {
		output, err := testRunCheckOutputWithOpts(t, env.gopts, testCase.opts, testCase.args)
		rtest.OK(t, err)

		hasOutput := strings.Contains(output, testCase.expectedOutput)
		rtest.Assert(t, hasOutput, `expected to find substring %q, but did not find it`, testCase.expectedOutput)
	}
}

func runCheckForTest(t *testing.T, gopts global.Options, opts CheckOptions) checkSummary {
	t.Helper()
	var summary checkSummary
	_, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		var runErr error
		summary, runErr = runCheck(ctx, opts, gopts, nil, gopts.Term)
		return runErr
	})
	rtest.OK(t, err)
	return summary
}

func TestCheckCheckpointResumeAndRangeStats(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata+"/0", []string{"for_cmd_ls"}, BackupOptions{}, env.gopts)
	testRunBackup(t, env.testdata+"/0", []string{"0/9"}, BackupOptions{}, env.gopts)

	checkpointFile := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	reportFile := filepath.Join(t.TempDir(), "damaged.txt")
	opts := CheckOptions{
		ReadData:         true,
		CheckpointFile:   checkpointFile,
		DamageReportFile: reportFile,
		Concurrency:      1,
	}

	first := runCheckForTest(t, env.gopts, opts)
	rtest.Assert(t, first.Data != nil, "expected data statistics")
	rtest.Assert(t, first.Data.PacksSelected > 0, "expected packs to be selected")
	rtest.Equals(t, first.Data.PacksSelected, first.Data.PacksChecked)
	rtest.Equals(t, 0, first.Data.PacksResumed)
	rtest.Assert(t, first.Data.Complete, "expected complete data phase")

	second := runCheckForTest(t, env.gopts, opts)
	rtest.Equals(t, first.Data.PacksSelected, second.Data.PacksSelected)
	rtest.Equals(t, 0, second.Data.PacksChecked)
	rtest.Equals(t, first.Data.PacksSelected, second.Data.PacksResumed)

	report, err := os.ReadFile(reportFile)
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(string(report), "0 damaged"), "report should contain stable statistics: %s", report)

	packs := listPacks(env.gopts, t)
	rtest.Assert(t, len(packs) > 0, "expected packs")
	packIDs := packs.List()
	target := packIDs[0]
	rangedOpts := CheckOptions{
		PackRange:   target.String() + ".." + target.String(),
		Concurrency: 1,
	}
	ranged := runCheckForTest(t, env.gopts, rangedOpts)
	rtest.Assert(t, ranged.Data != nil, "expected data statistics for ranged check")
	rtest.Equals(t, 1, ranged.Data.PacksSelected)
	rtest.Equals(t, 1, ranged.Data.PacksChecked)
}

func TestCheckWritesReadableDamagedPackList(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, env.testdata+"/0", []string{"for_cmd_ls"}, BackupOptions{}, env.gopts)

	packs := listPacks(env.gopts, t)
	rtest.Assert(t, len(packs) > 0, "expected packs")
	damagedID := packs.List()[0]
	getBackend := captureBackend(&env.gopts)

	rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, _, unlock, err := openWithExclusiveLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		be := getBackend()
		handle := backend.Handle{Type: backend.PackFile, Name: damagedID.String()}
		var data []byte
		rtest.OK(t, be.Load(ctx, handle, 0, 0, func(rd io.Reader) error {
			var readErr error
			data, readErr = io.ReadAll(rd)
			return readErr
		}))
		rtest.Assert(t, len(data) > 0, "expected pack data")
		data[len(data)-1] ^= 0xff
		rtest.OK(t, be.Remove(ctx, handle))
		return be.Save(ctx, handle, backend.NewByteReader(data, be.Hasher()))
	}))

	reportFile := filepath.Join(t.TempDir(), "damaged-packs.txt")
	checkpointFile := filepath.Join(t.TempDir(), "checkpoint.jsonl")
	opts := CheckOptions{
		ReadData:         true,
		DamageReportFile: reportFile,
		CheckpointFile:   checkpointFile,
		Concurrency:      1,
	}
	var summary checkSummary
	_, _, runErr := withCaptureStdoutStderr(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		var err error
		summary, err = runCheck(ctx, opts, gopts, nil, gopts.Term)
		return err
	})
	rtest.Assert(t, runErr != nil, "corrupt pack must fail check")
	rtest.Assert(t, summary.Data != nil && summary.Data.Errors > 0, "expected data error statistics")
	rtest.Equals(t, 1, summary.Data.PacksDamaged)
	rtest.Assert(t, len(summary.BrokenPacks) == 1 && summary.BrokenPacks[0] == damagedID.String(),
		"expected sorted broken pack list, got %#v", summary.BrokenPacks)

	report, err := os.ReadFile(reportFile)
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(string(report), damagedID.String()), "report must contain damaged pack ID: %s", report)
	rtest.Assert(t, strings.Contains(string(report), "damaged"), "report must classify damaged pack: %s", report)

	var resumedSummary checkSummary
	_, _, resumeErr := withCaptureStdoutStderr(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		var err error
		resumedSummary, err = runCheck(ctx, opts, gopts, nil, gopts.Term)
		return err
	})
	rtest.Assert(t, resumeErr != nil, "stored damaged result must keep failing resumed check")
	rtest.Equals(t, 0, resumedSummary.Data.PacksChecked)
	rtest.Assert(t, resumedSummary.Data.PacksReusedErrors > 0, "expected damaged result to be reused")
	rtest.Assert(t, len(resumedSummary.BrokenPacks) == 1 && resumedSummary.BrokenPacks[0] == damagedID.String(),
		"resumed run must keep stable damaged pack list, got %#v", resumedSummary.BrokenPacks)

	opts.RetryFailed = true
	var retrySummary checkSummary
	_, _, retryErr := withCaptureStdoutStderr(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		var err error
		retrySummary, err = runCheck(ctx, opts, gopts, nil, gopts.Term)
		return err
	})
	rtest.Assert(t, retryErr != nil, "rechecked corrupt pack must still fail")
	rtest.Assert(t, retrySummary.Data.PacksChecked >= 1, "expected failed pack to be rechecked")
}
