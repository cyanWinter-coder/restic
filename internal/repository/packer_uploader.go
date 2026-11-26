package repository

import (
	"context"
	"sync"
	"time"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

// savePacker implements saving a pack in the repository.
type savePacker interface {
	savePacker(ctx context.Context, t restic.BlobType, p *packer) error
}

type uploadTask struct {
	packer *packer
	tpe    restic.BlobType
}

// defaultUploadRetries is the number of upload attempts after the first one
// that failed.
const defaultUploadRetries = 5

// defaultUploadRetryBackoff returns the delay before the given retry attempt
// (attempt 1 is the first retry). The backoff is exponential and capped.
func defaultUploadRetryBackoff(attempt uint) time.Duration {
	const (
		initialInterval = 500 * time.Millisecond
		maxInterval     = 30 * time.Second
		multiplier      = 2
	)
	delay := initialInterval
	for i := uint(1); i < attempt && delay < maxInterval; i++ {
		delay *= multiplier
	}
	if delay > maxInterval {
		delay = maxInterval
	}
	return delay
}

type packerUploader struct {
	uploadQueue chan uploadTask

	mu sync.Mutex
	// inFlight tracks the packers that are currently queued or uploading, so
	// that the same pack is never uploaded twice. Every accepted packer is
	// recorded here; successfully uploaded packers are removed. Packers that
	// remain when the uploader stops are released by DiscardPending().
	inFlight map[*packer]struct{}

	// maxRetries is the number of upload retries after the initial attempt.
	maxRetries uint
	// backoff returns the delay before a retry attempt. It is a field so that
	// tests can use short, deterministic delays.
	backoff func(attempt uint) time.Duration
}

func newPackerUploader(ctx context.Context, wg *errgroup.Group, repo savePacker, connections uint) *packerUploader {
	pu := &packerUploader{
		uploadQueue: make(chan uploadTask),
		inFlight:    make(map[*packer]struct{}),
		maxRetries:  defaultUploadRetries,
		backoff:     defaultUploadRetryBackoff,
	}

	for i := 0; i < int(connections); i++ {
		wg.Go(func() error {
			for {
				select {
				case t, ok := <-pu.uploadQueue:
					if !ok {
						return nil
					}
					err := pu.uploadWithRetry(ctx, repo, t)
					if err != nil {
						// keep the packer tracked so DiscardPending releases its
						// temporary file once the uploader has stopped
						return err
					}
					pu.forget(t.packer)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	return pu
}

// DiscardPending closes the temporary files of all packers the uploader
// accepted but did not finish. It must be called after the worker group has
// stopped. Successfully uploaded packs have already closed their temp files
// during savePacker and are removed from the tracking set.
func (pu *packerUploader) DiscardPending() {
	pu.mu.Lock()
	pending := make([]*packer, 0, len(pu.inFlight))
	for p := range pu.inFlight {
		pending = append(pending, p)
	}
	pu.inFlight = make(map[*packer]struct{})
	pu.mu.Unlock()

	for _, p := range pending {
		p.cleanup()
	}
}

// uploadWithRetry uploads a pack, retrying transient failures with an
// exponential backoff. A pack that was already uploaded successfully is never
// uploaded again: the pack content is content-addressed, so a repeated upload
// would either hit the same object again or be rejected as an existing file.
func (pu *packerUploader) uploadWithRetry(ctx context.Context, repo savePacker, t uploadTask) error {
	var err error
	for attempt := uint(0); ; attempt++ {
		err = repo.savePacker(ctx, t.tpe, t.packer)
		if err == nil {
			if attempt > 0 {
				debug.Log("upload of pack succeeded after %d retries", attempt)
			}
			return nil
		}

		if attempt >= pu.maxRetries || ctx.Err() != nil {
			return err
		}

		delay := pu.backoff(attempt + 1)
		debug.Log("upload attempt %d for pack failed: %v, retrying in %v", attempt+1, err, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// QueuePacker enqueues p for uploading. A packer that is already queued or
// currently being uploaded is ignored, ensuring that each pack is uploaded at
// most once.
func (pu *packerUploader) QueuePacker(ctx context.Context, t restic.BlobType, p *packer) (err error) {
	if !pu.track(p) {
		debug.Log("pack is already queued for upload, skipping duplicate request")
		return nil
	}

	select {
	case <-ctx.Done():
		pu.forget(p)
		return ctx.Err()
	case pu.uploadQueue <- uploadTask{tpe: t, packer: p}:
	}

	return nil
}

func (pu *packerUploader) track(p *packer) bool {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	if _, ok := pu.inFlight[p]; ok {
		return false
	}
	pu.inFlight[p] = struct{}{}
	return true
}

func (pu *packerUploader) forget(p *packer) {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	delete(pu.inFlight, p)
}

func (pu *packerUploader) TriggerShutdown() {
	close(pu.uploadQueue)
}
