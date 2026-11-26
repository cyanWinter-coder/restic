package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/restic/restic/internal/backend/cache"
	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
)

func newCheckCommand(globalOptions *global.Options) *cobra.Command {
	var opts CheckOptions
	cmd := &cobra.Command{
		Use:   "check [flags]",
		Short: "Check the repository for errors",
		Long: `
The "check" command tests the repository for errors and reports any errors it
finds.

By default, check verifies the structural consistency and integrity of
snapshots, trees and pack files. To also verify the integrity of the actual
backed-up data, use the --read-data or --read-data-subset flags.

Structural checks and pack data checks can be run separately with
--structure-only and --data-only. Use --pack-range to restrict pack-level
conclusions to an inclusive pack ID range. Long data checks can be resumed with
--checkpoint, and --damaged-packs-file writes a stable readable list of bad
packs. check never deletes repository data itself.

By default, check creates a new temporary cache directory to verify data.
To reuse the existing cache, use the --with-cache flag.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			finalizeSnapshotFilter(&opts.SnapshotFilter)
			summary, err := runCheck(cmd.Context(), opts, *globalOptions, args, globalOptions.Term)
			if globalOptions.JSON {
				if err != nil && summary.NumErrors == 0 {
					summary.NumErrors = 1
				}
				globalOptions.Term.Print(ui.ToJSONString(summary))
			}
			return err
		},
		PreRunE: func(_ *cobra.Command, _ []string) error {
			return checkFlags(opts)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// CheckOptions bundles all options for the 'check' command.
type CheckOptions struct {
	ReadData       bool
	ReadDataSubset string
	CheckUnused    bool
	WithCache      bool

	// PackRange restricts pack-level conclusions to an inclusive pack ID range.
	PackRange string
	// CheckpointFile stores completed pack results and allows an interrupted
	// data verification to resume when the same path is supplied again.
	CheckpointFile string
	// DamageReportFile receives a stable, human-readable list of bad packs.
	DamageReportFile string
	// StructureOnly runs only structural checks.
	StructureOnly bool
	// DataOnly runs only pack data verification.
	DataOnly bool
	// Concurrency caps concurrent pack verification workers. Zero uses the
	// backend connection limit.
	Concurrency uint
	// RetryFailed checks packs whose latest checkpoint result was not verified
	// instead of reusing that terminal result on resume.
	RetryFailed bool

	data.SnapshotFilter
}

func (opts *CheckOptions) AddFlags(f *pflag.FlagSet) {
	f.BoolVar(&opts.ReadData, "read-data", false, "read all data blobs")
	f.StringVar(&opts.ReadDataSubset, "read-data-subset", "", "read a `subset` of data packs, specified as 'n/t' for specific part, or either 'x%' or 'x.y%' or a size in bytes with suffixes k/K, m/M, g/G, t/T for a random subset")
	var ignored bool
	f.BoolVar(&ignored, "check-unused", false, "find unused blobs")
	err := f.MarkDeprecated("check-unused", "`--check-unused` is deprecated and will be ignored")
	if err != nil {
		// MarkDeprecated only returns an error when the flag is not found
		panic(err)
	}
	f.BoolVar(&opts.WithCache, "with-cache", false, "use existing cache, only read uncached data from repository")
	f.StringVar(&opts.PackRange, "pack-range", "", "only check packs in inclusive hex `range`, e.g. 00aa..ff00 or 00aa-ff00; abbreviated IDs are allowed")
	f.StringVar(&opts.PackRange, "range", "", "alias for --pack-range")
	f.StringVar(&opts.CheckpointFile, "checkpoint", "", "resumable JSONL `file` recording completed pack checks; it is created if it does not exist")
	f.StringVar(&opts.CheckpointFile, "progress-file", "", "alias for --checkpoint")
	f.StringVar(&opts.CheckpointFile, "state-file", "", "alias for --checkpoint")
	f.StringVar(&opts.DamageReportFile, "damaged-packs-file", "", "write a readable list of damaged or unreadable packs to `file`")
	f.StringVar(&opts.DamageReportFile, "failed-packs-file", "", "alias for --damaged-packs-file")
	f.StringVar(&opts.DamageReportFile, "damage-report", "", "alias for --damaged-packs-file")
	f.BoolVar(&opts.StructureOnly, "structure-only", false, "run only index, pack metadata, snapshot and tree checks")
	f.BoolVar(&opts.DataOnly, "data-only", false, "run only pack data verification (implies --read-data unless a subset/range is given)")
	f.UintVar(&opts.Concurrency, "concurrency", 0, "maximum number of packs to verify concurrently (default: backend connection limit)")
	f.UintVar(&opts.Concurrency, "workers", 0, "alias for --concurrency")
	f.UintVar(&opts.Concurrency, "pack-workers", 0, "alias for --concurrency")
	f.BoolVar(&opts.RetryFailed, "retry-failed", false, "recheck packs with damaged or failed checkpoint results when resuming")
	for _, name := range []string{"range", "progress-file", "state-file", "failed-packs-file", "damage-report", "workers", "pack-workers"} {
		if flag := f.Lookup(name); flag != nil {
			flag.Hidden = true
		}
	}
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, true)
}

func checkFlags(opts CheckOptions) error {
	if opts.StructureOnly && opts.DataOnly {
		return errors.Fatal("check flags --structure-only and --data-only cannot be used together")
	}
	if opts.StructureOnly && opts.CheckpointFile != "" {
		return errors.Fatal("check flag --checkpoint records data checks and cannot be used with --structure-only")
	}
	if opts.CheckpointFile != "" && opts.CheckpointFile == opts.DamageReportFile {
		return errors.Fatal("check flags --checkpoint and --damaged-packs-file must use different files")
	}
	if opts.PackRange != "" {
		if _, err := checker.ParsePackRange(opts.PackRange); err != nil {
			return errors.Fatalf("check flag --pack-range is invalid: %v", err)
		}
	}
	if opts.ReadData && opts.ReadDataSubset != "" {
		return errors.Fatal("check flags --read-data and --read-data-subset cannot be used together")
	}
	if opts.ReadDataSubset != "" {
		dataSubset, err := stringToIntSlice(opts.ReadDataSubset)
		argumentError := errors.Fatal("check flag --read-data-subset has invalid value, please see documentation")
		if err == nil {
			if len(dataSubset) != 2 {
				return argumentError
			}
			if dataSubset[0] == 0 || dataSubset[1] == 0 || dataSubset[0] > dataSubset[1] {
				return errors.Fatal("check flag --read-data-subset=n/t values must be positive integers, and n <= t, e.g. --read-data-subset=1/2")
			}
			if dataSubset[1] > totalBucketsMax {
				return errors.Fatalf("check flag --read-data-subset=n/t t must be at most %d", totalBucketsMax)
			}
		} else if strings.HasSuffix(opts.ReadDataSubset, "%") {
			percentage, err := parsePercentage(opts.ReadDataSubset)
			if err != nil {
				return argumentError
			}

			if percentage <= 0.0 || percentage > 100.0 {
				return errors.Fatal(
					"check flag --read-data-subset=x% x must be above 0.0% and at most 100.0%")
			}

		} else {
			fileSize, err := ui.ParseBytes(opts.ReadDataSubset)
			if err != nil {
				return argumentError
			}
			if fileSize <= 0.0 {
				return errors.Fatal(
					"check flag --read-data-subset=n n must be above 0")
			}

		}
	}

	return nil
}

// See doReadData in runCheck below for why this is 256.
const totalBucketsMax = 256

// stringToIntSlice converts string to []uint, using '/' as element separator
func stringToIntSlice(param string) (split []uint, err error) {
	if param == "" {
		return nil, nil
	}
	parts := strings.Split(param, "/")
	result := make([]uint, len(parts))
	for idx, part := range parts {
		uintval, err := strconv.ParseUint(part, 10, 0)
		if err != nil {
			return nil, err
		}
		result[idx] = uint(uintval)
	}
	return result, nil
}

// ParsePercentage parses a percentage string of the form "X%" where X is a float constant,
// and returns the value of that constant. It does not check the range of the value.
func parsePercentage(s string) (float64, error) {
	if !strings.HasSuffix(s, "%") {
		return 0, errors.Errorf(`parsePercentage: %q does not end in "%%"`, s)
	}
	s = s[:len(s)-1]

	p, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errors.Errorf("parsePercentage: %v", err)
	}
	return p, nil
}

// prepareCheckCache configures a special cache directory for check.
//
//   - if --with-cache is specified, the default cache is used
//   - if the user explicitly requested --no-cache, we don't use any cache
//   - if the user provides --cache-dir, we use a cache in a temporary sub-directory of the specified directory and the sub-directory is deleted after the check
//   - by default, we use a cache in a temporary directory that is deleted after the check
func prepareCheckCache(opts CheckOptions, gopts *global.Options, printer restic.Printer) (cleanup func()) {
	cleanup = func() {}
	if opts.WithCache {
		// use the default cache, no setup needed
		return cleanup
	}

	if gopts.NoCache {
		// don't use any cache, no setup needed
		return cleanup
	}

	cachedir := gopts.CacheDir
	if cachedir == "" {
		cachedir = cache.EnvDir()
	}

	if cachedir != "" {
		// use a cache in a temporary directory
		err := os.MkdirAll(cachedir, 0755)
		if err != nil {
			printer.E("unable to create cache directory %s, disabling cache: %v", cachedir, err)
			gopts.NoCache = true
			return cleanup
		}
	}
	tempdir, err := os.MkdirTemp(cachedir, "restic-check-cache-")
	if err != nil {
		// if an error occurs, don't use any cache
		printer.E("unable to create temporary directory for cache during check, disabling cache: %v\n", err)
		gopts.NoCache = true
		return cleanup
	}

	gopts.CacheDir = tempdir
	printer.P("using temporary cache in %v\n", tempdir)

	cleanup = func() {
		err := os.RemoveAll(tempdir)
		if err != nil {
			printer.E("error removing temporary cache directory: %v\n", err)
		}
	}

	return cleanup
}

func writePackReportFile(path, repoID, scope string, results []checker.PackResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	reportErr := checker.WritePackReport(f, repoID, scope, results)
	closeErr := f.Close()
	if reportErr != nil {
		return reportErr
	}
	return closeErr
}

func runCheck(ctx context.Context, opts CheckOptions, gopts global.Options, args []string, term ui.Terminal) (summary checkSummary, err error) {
	summary = checkSummary{
		MessageType: "summary",
		Structure:   &structureCheckSummary{Status: "not_run"},
		Data:        &dataCheckSummary{Status: "not_run"},
	}
	if err := checkFlags(opts); err != nil {
		return summary, err
	}

	idRange := packRange{}
	if opts.PackRange != "" {
		idRange, err = parsePackRange(opts.PackRange)
		if err != nil {
			return summary, errors.Fatalf("check flag --pack-range is invalid: %v", err)
		}
	}

	runStructure := !opts.DataOnly
	wantsData := opts.DataOnly || opts.ReadData || opts.ReadDataSubset != "" ||
		opts.PackRange != "" || opts.CheckpointFile != ""
	runData := wantsData && !opts.StructureOnly

	var printer restic.Printer
	if !gopts.JSON {
		printer = progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)
	} else {
		printer = newJSONErrorPrinter(term)
	}

	findings := newPackResultTracker()
	var checkpoint *checker.Checkpoint
	repoID := "unknown"
	dataScope := "unconfigured"
	defer func() {
		if opts.DamageReportFile != "" {
			results := findings.Results()
			if checkpoint != nil {
				results = checkpoint.Results()
			}
			if reportErr := writePackReportFile(opts.DamageReportFile, repoID, dataScope, results); reportErr != nil {
				printer.E("unable to write damaged packs report: %v\n", reportErr)
				if err == nil {
					err = errors.Fatalf("write damaged packs report: %v", reportErr)
				}
			} else {
				printer.P("damaged packs report written to %s\n", opts.DamageReportFile)
			}
		}
		if checkpoint != nil {
			if closeErr := checkpoint.Close(); closeErr != nil {
				printer.E("unable to close checkpoint: %v\n", closeErr)
				if err == nil {
					err = errors.Fatalf("close check checkpoint: %v", closeErr)
				}
			}
		}
	}()

	cleanup := prepareCheckCache(opts, &gopts, printer)
	defer cleanup()

	if !gopts.NoLock {
		printer.P("create exclusive lock for repository\n")
	}
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, gopts.NoLock, printer)
	if err != nil {
		return summary, err
	}
	defer unlock()
	repoID = repo.Config().ID

	chkr := checker.New(repo, opts.CheckUnused)
	err = chkr.LoadSnapshots(ctx, &opts.SnapshotFilter, args)
	if err != nil {
		return summary, err
	}

	scopeParts := []string{"check-v1"}
	if chkr.IsFiltered() {
		scopeParts = append(scopeParts, "snapshot-filtered")
		scopeParts = append(scopeParts, args...)
		scopeParts = append(scopeParts, fmt.Sprintf("filter=%#v", opts.SnapshotFilter))
	}
	if opts.PackRange != "" {
		scopeParts = append(scopeParts, "range="+idRange.String())
	}
	if opts.ReadDataSubset != "" {
		scopeParts = append(scopeParts, "subset="+opts.ReadDataSubset)
	}
	dataScope = strings.Join(scopeParts, ";")

	if opts.CheckpointFile != "" {
		checkpoint, err = checker.OpenCheckpoint(opts.CheckpointFile, repoID, dataScope)
		if err != nil {
			return summary, errors.Fatalf("unable to open check checkpoint: %v", err)
		}
		verified, damaged, failed := checkpoint.Counts()
		summary.Data.CheckpointFile = opts.CheckpointFile
		printer.P("resuming check from %s: %d verified, %d damaged, %d failed pack results recorded\n",
			opts.CheckpointFile, verified, damaged, failed)
	}
	summary.Data.ReportFile = opts.DamageReportFile

	printer.P("load indexes\n")
	hints, errs := chkr.LoadIndex(ctx, printer)
	if ctx.Err() != nil {
		return summary, ctx.Err()
	}

	errorsFound := false
	salvagePacks := restic.NewIDSet()

	for _, hint := range hints {
		switch hint := hint.(type) {
		case *repository.ErrIncompletePackEntry:
			if opts.PackRange != "" && !idRange.Contains(hint.PackID) {
				printer.V("skipping out-of-range index hint: %v\n", hint)
				continue
			}
			printer.E("%s", hint.Error())
			salvagePacks.Insert(hint.PackID)
			errorsFound = true
			summary.NumErrors++
			summary.Structure.Errors++
		case *repository.ErrDuplicatePacks:
			if opts.PackRange != "" && !idRange.Contains(hint.PackID) {
				continue
			}
			printer.S("%s", hint.Error())
			summary.HintRepairIndex = true
		case *repository.ErrMixedPack:
			if opts.PackRange != "" && !idRange.Contains(hint.PackID) {
				continue
			}
			printer.S("%s", hint.Error())
			summary.HintPrune = true
		default:
			printer.E("error: %v\n", hint)
			errorsFound = true
			summary.NumErrors++
			summary.Structure.Errors++
		}
	}

	if summary.HintRepairIndex {
		printer.S("Duplicate packs are non-critical, you can run `restic repair index' to correct this.\n")
	}
	if summary.HintPrune {
		printer.S("Mixed packs with tree and data blobs are non-critical, you can run `restic prune` to correct this.\n")
	}

	if len(errs) > 0 {
		for _, err := range errs {
			printer.E("error: %v\n", err)
		}

		summary.NumErrors += len(errs)
		summary.Structure.Errors += len(errs)
		summary.Structure.Status = "errors"
		summary.HintRepairIndex = true
		printer.E("\nThe repository index is damaged and must be repaired. You must run `restic repair index' to correct this.\n\n")
		return summary, errors.Fatal("repository contains errors")
	}

	orphanedPacks := 0
	if runStructure {
		errChan := make(chan error)

		if opts.PackRange == "" {
			printer.P("check all packs\n")
		} else {
			printer.P("check packs in range %s\n", idRange.String())
		}
		go chkr.Packs(ctx, errChan)

		for err := range errChan {
			var packErr *repository.ErrPackMetadata
			if errors.As(err, &packErr) {
				if opts.PackRange != "" && !idRange.Contains(packErr.ID) {
					printer.V("skipping out-of-range pack metadata result: %v\n", err)
					continue
				}
				if packErr.Orphaned {
					orphanedPacks++
					printer.V("%v\n", err)
					continue
				}

				if packErr.Truncated || packErr.Missing {
					salvagePacks.Insert(packErr.ID)
				}
				result := checker.NewPackResult(packErr.ID, 0, err)
				result.Status = checker.PackDamaged
				findings.AddResult(result)
				if checkpoint != nil {
					if recordErr := checkpoint.Record(result); recordErr != nil {
						printer.E("checkpoint write failed: %v\n", recordErr)
					}
				}
				errorsFound = true
				summary.NumErrors++
				summary.Structure.Errors++
				printer.E("%v\n", err)
			} else {
				errorsFound = true
				summary.Structure.Errors++
				printer.E("%v\n", err)
			}
		}

		if orphanedPacks > 0 {
			summary.HintPrune = true
			if !errorsFound {
				// hide notice if repository is damaged
				printer.P("%d additional files were found in the repo, which likely contain duplicate data.\nThis is non-critical, you can run `restic prune` to correct this.\n", orphanedPacks)
			}
		}
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}
	} else {
		printer.V("skipping structural pack metadata checks because --data-only is set\n")
	}

	var brokenSnapshots []string
	if runStructure {
		printer.P("check snapshots, trees and blobs\n")
		errChan := make(chan error)
		var wg sync.WaitGroup
		structureCounter := &countingCounter{Counter: printer.NewCounter("snapshots")}

		wg.Go(func() {
			defer structureCounter.Done()
			chkr.Structure(ctx, structureCounter, errChan)
		})

		for err := range errChan {
			errorsFound = true
			switch e := err.(type) {
			case *checker.TreeError:
				printer.E("error for tree %v:\n", e.ID.Str())
				for _, treeErr := range e.Errors {
					summary.NumErrors++
					summary.Structure.Errors++
					printer.E("  %v\n", treeErr)
				}
			case *checker.SnapshotError:
				summary.NumErrors++
				summary.Structure.Errors++
				printer.E("snapshot error %v: %v", e.ID, e.Message)
				brokenSnapshots = append(brokenSnapshots, e.ID)
			default:
				summary.NumErrors++
				summary.Structure.Errors++
				printer.E("error: %v\n", err)
			}
		}

		// Wait for the progress bar to be complete before printing more below.
		// Must happen after `errChan` is read from in the above loop to avoid
		// deadlocking in the case of errors.
		wg.Wait()
		checked, total := structureCounter.totals()
		summary.Structure.SnapshotsChecked = checked
		summary.Structure.SnapshotsTotal = total
		if summary.Structure.Errors > 0 {
			summary.Structure.Status = "errors"
		} else {
			summary.Structure.Status = "ok"
		}
		printer.P("structure check: %d/%d snapshots/root trees checked, %d errors\n",
			checked, total, summary.Structure.Errors)
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}
	} else {
		summary.Structure.Status = "not_run"
		printer.P("structure check: skipped because --data-only was set\n")
	}

	// the following block only used for tests
	if opts.CheckUnused && runStructure {
		unused, err := chkr.UnusedBlobs(ctx)
		if err != nil {
			return summary, err
		}
		for _, id := range unused {
			printer.P("unused blob %v\n", id)
			errorsFound = true
		}
	}

	if runData {
		dataOpts := opts
		if !dataOpts.ReadData && dataOpts.ReadDataSubset == "" {
			// --data-only and --checkpoint without another selector mean all
			// packs. --pack-range is handled by the composing filter below.
			dataOpts.ReadData = opts.PackRange == ""
		}
		baseFilter, err := buildPacksFilter(dataOpts, printer, chkr.IsFiltered())
		if err != nil {
			return summary, err
		}
		if baseFilter == nil {
			baseFilter = func(packs map[restic.ID]int64) map[restic.ID]int64 { return packs }
		}

		summary.Data.Scope = dataScope
		summary.Data.Status = "ok"
		summary.Data.Partial = opts.PackRange != "" || opts.ReadDataSubset != "" || chkr.IsFiltered()
		var checkedCount atomic.Uint64
		var verifiedBytes atomic.Int64
		reusedProblemPacks := restic.NewIDSet()

		packFilter := func(allPacks map[restic.ID]int64) map[restic.ID]int64 {
			summary.Data.PacksTotal = len(allPacks)
			scopedPacks := allPacks
			if opts.PackRange != "" {
				scopedPacks = selectPacksByRange(allPacks, idRange)
			}
			selectedPacks := baseFilter(scopedPacks)
			summary.Data.PacksSelected = len(selectedPacks)
			summary.Data.PacksOutOfScope = summary.Data.PacksTotal - summary.Data.PacksSelected

			packsToCheck := make(map[restic.ID]int64, len(selectedPacks))
			resumed := 0
			reusedErrors := 0
			for id, size := range selectedPacks {
				if checkpoint != nil {
					if result, ok := checkpoint.Result(id); ok {
						if opts.RetryFailed && result.Status != checker.PackVerified {
							packsToCheck[id] = size
							continue
						}

						resumed++
						if result.Status != checker.PackVerified {
							reusedErrors++
							reusedProblemPacks.Insert(id)
							if result.Status == checker.PackDamaged {
								salvagePacks.Insert(id)
							}
						}
						continue
					}
				}
				packsToCheck[id] = size
			}
			summary.Data.PacksResumed = resumed
			summary.Data.PacksReusedErrors = reusedErrors
			return packsToCheck
		}

		errChan := make(chan error)
		go chkr.ReadPacksWithOptions(ctx, checker.ReadPacksOptions{
			Filter:      packFilter,
			Printer:     printer,
			Concurrency: opts.Concurrency,
			OnPackComplete: func(id restic.ID, size int64, packErr error) {
				checkedCount.Add(1)
				if packErr == nil {
					verifiedBytes.Add(size)
				}
				result := findings.Record(id, size, packErr)
				if checkpoint != nil {
					if recordErr := checkpoint.Record(result); recordErr != nil {
						printer.E("checkpoint write failed: %v\n", recordErr)
					}
				}
			},
		}, errChan)

		for err := range errChan {
			errorsFound = true
			summary.NumErrors++
			summary.Data.Errors++
			printer.E("%v\n", err)
			var packDataErr *repository.ErrPackData
			if errors.As(err, &packDataErr) {
				salvagePacks.Insert(packDataErr.PackID)
			}
		}

		for _, packID := range reusedProblemPacks.List() {
			status := "failed"
			if result, ok := checkpoint.Result(packID); ok {
				status = string(result.Status)
			}
			errorsFound = true
			summary.NumErrors++
			summary.Data.Errors++
			printer.E("pack %s: reusing %s result from checkpoint; pass --retry-failed to verify it again\n",
				packID, status)
		}

		summary.Data.PacksChecked = int(checkedCount.Load())
		summary.Data.BytesVerified = verifiedBytes.Load()
		if summary.Data.Errors > 0 {
			summary.Data.Status = "errors"
		}
		printer.P("data check: %d packs selected, %d checked now, %d resumed from checkpoint, %d errors\n",
			summary.Data.PacksSelected, summary.Data.PacksChecked, summary.Data.PacksResumed, summary.Data.Errors)
		if gopts.Limits.DownloadKb > 0 {
			printer.P("download bandwidth was limited to %d KiB/s by --limit-download\n", gopts.Limits.DownloadKb)
		}
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}
		summary.Data.Complete = true
	} else {
		summary.Data.Status = "not_run"
		printer.P("data check: skipped (use --read-data, --read-data-subset, --pack-range or --data-only)\n")
	}

	problemResults := findings.Results()
	if checkpoint != nil {
		problemResults = checkpoint.Results()
	}
	var damagedIDs []string
	damagedCount, failedCount := 0, 0
	for _, result := range problemResults {
		if result.Status == checker.PackVerified {
			continue
		}
		damagedIDs = append(damagedIDs, result.ID)
		if result.Status == checker.PackDamaged {
			damagedCount++
		} else {
			failedCount++
		}
	}
	sort.Strings(damagedIDs)
	summary.Data.DamagedPacks = damagedIDs
	summary.Data.PacksDamaged = damagedCount
	summary.Data.PacksFailed = failedCount

	if len(damagedIDs) > 0 {
		printer.E("\nDamaged or unreadable packs (restic has not deleted or modified any data):\n")
		for _, result := range problemResults {
			if result.Status == checker.PackVerified {
				continue
			}
			if result.Error != "" {
				printer.E("  %s [%s]: %s\n", result.ID, result.Status, result.Error)
			} else {
				printer.E("  %s [%s]\n", result.ID, result.Status)
			}
		}
	}

	if len(salvagePacks) > 0 {
		printer.E("\nThe repository contains damaged pack files. These damaged files must be removed to repair the repository. This can be done using the following commands. Please read the troubleshooting guide at https://restic.readthedocs.io/en/stable/077_troubleshooting.html first.\n\n")
		salvageIDs := make([]string, 0, len(salvagePacks))
		for id := range salvagePacks {
			salvageIDs = append(salvageIDs, id.String())
		}
		sort.Strings(salvageIDs)
		summary.BrokenPacks = salvageIDs
		printer.E("restic repair packs %v\nrestic repair snapshots --forget\n\n", strings.Join(summary.BrokenPacks, " "))
		printer.E("Damaged pack files can be caused by backend problems, hardware problems or bugs in restic. Please open an issue at https://github.com/restic/restic/issues/new/choose for further troubleshooting!\n")
	}

	if len(brokenSnapshots) > 0 {
		printer.E("\nThe repository contains damaged snapshot files. These damaged files must be removed to repair the repository. This can be done using the following commands. Please read the troubleshooting guide at https://restic.readthedocs.io/en/stable/077_troubleshooting.html first.\n\n")
		printer.E("restic repair snapshots --forget %s\n\n", strings.Join(brokenSnapshots, " "))
		printer.E("Damaged snapshot files can be caused by backend problems, hardware problems or bugs in restic. Please open an issue at https://github.com/restic/restic/issues/new/choose for further troubleshooting!\n")
	}

	if ctx.Err() != nil {
		return summary, ctx.Err()
	}

	if errorsFound {
		if len(salvagePacks) == 0 && len(brokenSnapshots) == 0 {
			printer.E("\nThe repository is damaged and must be repaired. Please follow the troubleshooting guide at https://restic.readthedocs.io/en/stable/077_troubleshooting.html .\n\n")
		}
		return summary, errors.Fatal("repository contains errors")
	}
	if runData && summary.Data.PacksTotal > summary.Data.PacksSelected {
		printer.P("no errors were found in the selected scope; %d pack(s) outside that scope were not checked and do not affect this conclusion\n",
			summary.Data.PacksTotal-summary.Data.PacksSelected)
	} else if opts.PackRange != "" || opts.ReadDataSubset != "" {
		printer.P("no errors were found in the selected scope\n")
	} else {
		printer.P("no errors were found\n")
	}
	return summary, nil
}

func buildPacksFilter(opts CheckOptions, printer restic.Printer,
	filteredStatus bool) (func(packs map[restic.ID]int64) map[restic.ID]int64, error) {
	typeData := ""
	if filteredStatus {
		typeData = "filtered "
	}
	switch {
	case opts.ReadData:
		return func(packs map[restic.ID]int64) map[restic.ID]int64 {
			printer.P("read all %sdata", typeData)
			return packs
		}, nil
	case opts.ReadDataSubset != "":
		dataSubset, err := stringToIntSlice(opts.ReadDataSubset)
		if err == nil {
			bucket := dataSubset[0]
			totalBuckets := dataSubset[1]
			return func(packs map[restic.ID]int64) map[restic.ID]int64 {
				packCount := uint64(len(packs))
				packs = selectPacksByBucket(packs, bucket, totalBuckets)
				printer.P("read group #%d of %d %sdata packs (out of total %d packs in %d groups", bucket, len(packs), typeData, packCount, totalBuckets)
				return packs
			}, nil
		} else if strings.HasSuffix(opts.ReadDataSubset, "%") {
			percentage, err := parsePercentage(opts.ReadDataSubset)
			if err != nil {
				return nil, err
			}
			return func(packs map[restic.ID]int64) map[restic.ID]int64 {
				printer.P("read %.1f%% of %spackfiles", percentage, typeData)
				return selectRandomPacksByPercentage(packs, percentage)
			}, nil
		}

		repoSize := int64(0)
		return func(packs map[restic.ID]int64) map[restic.ID]int64 {
			for _, size := range packs {
				repoSize += size
			}
			subsetSize, _ := ui.ParseBytes(opts.ReadDataSubset)
			if subsetSize > repoSize {
				subsetSize = repoSize
			}
			if repoSize > 0 {
				packs = selectRandomPacksByFileSize(packs, subsetSize, repoSize)
			}
			percentage := float64(subsetSize) / float64(repoSize) * 100.0
			if repoSize == 0 {
				percentage = 100
			}
			printer.P("read %d bytes (%.1f%%) of %sdata packs\n", subsetSize, percentage, typeData)
			return packs
		}, nil
	}

	return nil, nil
}

// packRange is the command-level alias for the reusable checker range type.
type packRange = checker.PackRange

func parsePackRange(s string) (packRange, error) {
	return checker.ParsePackRange(s)
}

func selectPacksByRange(packs map[restic.ID]int64, r packRange) map[restic.ID]int64 {
	return checker.SelectPacksByRange(packs, r)
}

// selectPacksByBucket selects subsets of packs by ranges of buckets.
func selectPacksByBucket(allPacks map[restic.ID]int64, bucket, totalBuckets uint) map[restic.ID]int64 {
	packs := make(map[restic.ID]int64)
	for pack, size := range allPacks {
		// If we ever check more than the first byte
		// of pack, update totalBucketsMax.
		if (uint(pack[0]) % totalBuckets) == (bucket - 1) {
			packs[pack] = size
		}
	}
	return packs
}

// selectRandomPacksByPercentage selects the given percentage of packs which are randomly chosen.
func selectRandomPacksByPercentage(allPacks map[restic.ID]int64, percentage float64) map[restic.ID]int64 {
	packCount := len(allPacks)
	packsToCheck := int(float64(packCount) * (percentage / 100.0))
	if packCount > 0 && packsToCheck < 1 {
		packsToCheck = 1
	}
	timeNs := time.Now().UnixNano()
	r := rand.New(rand.NewSource(timeNs))
	idx := r.Perm(packCount)

	var keys []restic.ID
	for k := range allPacks {
		keys = append(keys, k)
	}

	packs := make(map[restic.ID]int64)

	for i := 0; i < packsToCheck; i++ {
		id := keys[idx[i]]
		packs[id] = allPacks[id]
	}
	return packs
}

func selectRandomPacksByFileSize(allPacks map[restic.ID]int64, subsetSize int64, repoSize int64) map[restic.ID]int64 {
	subsetPercentage := (float64(subsetSize) / float64(repoSize)) * 100.0
	packs := selectRandomPacksByPercentage(allPacks, subsetPercentage)
	return packs
}

type checkSummary struct {
	MessageType     string                 `json:"message_type"` // "summary"
	NumErrors       int                    `json:"num_errors"`
	BrokenPacks     []string               `json:"broken_packs"`         // run "restic repair packs ID..." and "restic repair snapshots --forget" to remove damaged files
	HintRepairIndex bool                   `json:"suggest_repair_index"` // run "restic repair index"
	HintPrune       bool                   `json:"suggest_prune"`        // run "restic prune"
	Structure       *structureCheckSummary `json:"structure,omitempty"`
	Data            *dataCheckSummary      `json:"data,omitempty"`
}

type structureCheckSummary struct {
	Status           string `json:"status"` // "not_run", "ok", or "errors"
	Errors           int    `json:"errors"`
	SnapshotsTotal   uint64 `json:"snapshots_total"`
	SnapshotsChecked uint64 `json:"snapshots_checked"`
}

type dataCheckSummary struct {
	Status            string   `json:"status"` // "not_run", "ok", or "errors"
	Errors            int      `json:"errors"`
	Scope             string   `json:"scope,omitempty"`
	Partial           bool     `json:"partial"`
	Complete          bool     `json:"complete"`
	PacksTotal        int      `json:"packs_total"`
	PacksSelected     int      `json:"packs_selected"`
	PacksOutOfScope   int      `json:"packs_out_of_scope"`
	PacksChecked      int      `json:"packs_checked"`
	PacksResumed      int      `json:"packs_resumed"`
	PacksReusedErrors int      `json:"packs_reused_errors"`
	PacksDamaged      int      `json:"packs_damaged"`
	PacksFailed       int      `json:"packs_failed"`
	BytesVerified     int64    `json:"bytes_verified"`
	DamagedPacks      []string `json:"damaged_packs,omitempty"`
	CheckpointFile    string   `json:"checkpoint_file,omitempty"`
	ReportFile        string   `json:"report_file,omitempty"`
}

type checkError struct {
	MessageType string `json:"message_type"` // "error"
	Message     string `json:"message"`
}

// packResultTracker collects results for packs checked in this process. It is
// used even when no checkpoint file was requested and backs the damage report.
type packResultTracker struct {
	mu      sync.Mutex
	results map[restic.ID]checker.PackResult
}

func newPackResultTracker() *packResultTracker {
	return &packResultTracker{results: make(map[restic.ID]checker.PackResult)}
}

func (t *packResultTracker) Record(id restic.ID, size int64, err error) checker.PackResult {
	result := checker.NewPackResult(id, size, err)
	t.mu.Lock()
	t.results[id] = result
	t.mu.Unlock()
	return result
}

func (t *packResultTracker) AddResult(result checker.PackResult) {
	id, err := restic.ParseID(result.ID)
	if err != nil {
		return
	}
	t.mu.Lock()
	t.results[id] = result
	t.mu.Unlock()
}

func (t *packResultTracker) Results() []checker.PackResult {
	t.mu.Lock()
	results := make([]checker.PackResult, 0, len(t.results))
	for _, result := range t.results {
		results = append(results, result)
	}
	t.mu.Unlock()
	sort.Slice(results, func(i, j int) bool { return results[i].ID < results[j].ID })
	return results
}

// countingCounter retains exact totals even in JSON mode, where the printer
// returns a no-op progress counter.
type countingCounter struct {
	restic.Counter
	max  uint64
	done uint64
}

func (c *countingCounter) Add(n uint64) {
	atomic.AddUint64(&c.done, n)
	if c.Counter != nil {
		c.Counter.Add(n)
	}
}

func (c *countingCounter) SetMax(n uint64) {
	atomic.StoreUint64(&c.max, n)
	if c.Counter != nil {
		c.Counter.SetMax(n)
	}
}

func (c *countingCounter) totals() (done, max uint64) {
	return atomic.LoadUint64(&c.done), atomic.LoadUint64(&c.max)
}

type jsonErrorPrinter struct {
	term ui.Terminal
}

func newJSONErrorPrinter(term ui.Terminal) *jsonErrorPrinter {
	return &jsonErrorPrinter{
		term: term,
	}
}

func (*jsonErrorPrinter) NewCounter(_ string) restic.Counter {
	return restic.NoopCounter
}

func (*jsonErrorPrinter) NewCounterTerminalOnly(_ string) restic.Counter {
	return restic.NoopCounter
}

func (p *jsonErrorPrinter) E(msg string, args ...any) {
	status := checkError{
		MessageType: "error",
		Message:     fmt.Sprintf(msg, args...),
	}
	p.term.Error(ui.ToJSONString(status))
}
func (*jsonErrorPrinter) S(_ string, _ ...any)  {}
func (*jsonErrorPrinter) P(_ string, _ ...any)  {}
func (*jsonErrorPrinter) PT(_ string, _ ...any) {}
func (*jsonErrorPrinter) V(_ string, _ ...any)  {}
func (*jsonErrorPrinter) VV(_ string, _ ...any) {}
