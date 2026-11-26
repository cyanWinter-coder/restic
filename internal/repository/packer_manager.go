package repository

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"math/big"
	"os"
	"sync"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/repository/hashing"
	"github.com/restic/restic/internal/restic"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/fileio"
	"github.com/restic/restic/internal/repository/crypto"
	"github.com/restic/restic/internal/repository/pack"
)

// packer holds a pack.packer together with a hash writer.
type packer struct {
	*pack.Packer
	tmpfile *os.File
	bufWr   *bufio.Writer

	// finalized is set once Finalize() has run. Finalizing is idempotent so
	// that a retried upload does not corrupt the pack header.
	finalized bool
	// id is the content hash of the finalized pack. It is cached so that a
	// retried upload addresses the same backend object.
	id restic.ID
	// uploaded tracks whether the pack has been stored in the backend.
	uploaded bool
}

// finalizePack finalizes the pack and computes its content hash. It is safe
// to call multiple times: the result of the first successful call is reused.
func (p *packer) finalizePack() (restic.ID, error) {
	if p.finalized {
		return p.id, nil
	}

	if err := p.Packer.Finalize(); err != nil {
		return restic.ID{}, err
	}
	if err := p.bufWr.Flush(); err != nil {
		return restic.ID{}, err
	}

	// calculate sha256 hash in a second pass
	if _, err := p.tmpfile.Seek(0, io.SeekStart); err != nil {
		return restic.ID{}, err
	}
	hr := hashing.NewReader(p.tmpfile, sha256.New())
	if _, err := io.Copy(io.Discard, hr); err != nil {
		return restic.ID{}, err
	}

	p.id = restic.IDFromHash(hr.Sum(nil))
	p.finalized = true
	return p.id, nil
}

// cleanup removes the temporary file backing the pack. It is safe to call
// multiple times and after a failed call.
func (p *packer) cleanup() {
	if p.tmpfile == nil {
		return
	}
	// the temp file is already unlinked on Unix; closing the descriptor frees it
	err := p.tmpfile.Close()
	if err != nil {
		debug.Log("closing temp file failed: %v", err)
	}
	p.tmpfile = nil
}

// packerManager keeps a list of open packs and creates new on demand.
type packerManager struct {
	tpe     restic.BlobType
	key     *crypto.Key
	queueFn func(ctx context.Context, t restic.BlobType, p *packer) error

	pm       sync.Mutex
	packers  []*packer
	packSize uint

	// registry holds every open pack handle created by this manager. It
	// guarantees that a pack handle is never created twice for the same logical
	// pack: repeat lookups return the same *packer. Handles are removed when
	// their pack is queued for upload or discarded.
	registry map[*packer]struct{}
}

const defaultPackerCount = 2

// newPackerManager returns a new packer manager which writes temporary files
// to a temporary directory
func newPackerManager(key *crypto.Key, tpe restic.BlobType, packSize uint, packerCount int, queueFn func(ctx context.Context, t restic.BlobType, p *packer) error) *packerManager {
	return &packerManager{
		tpe:      tpe,
		key:      key,
		queueFn:  queueFn,
		packers:  make([]*packer, packerCount),
		packSize: packSize,
		registry: make(map[*packer]struct{}),
	}
}

// registerPacker records p as an open pack handle. It must be called with pm
// held.
func (r *packerManager) registerPacker(p *packer) {
	r.registry[p] = struct{}{}
}

// unregisterPacker removes p from the open handle registry. It must be called
// with pm held.
func (r *packerManager) unregisterPacker(p *packer) {
	delete(r.registry, p)
}

// openPackers returns a snapshot of all currently open pack handles.
func (r *packerManager) openPackers() []*packer {
	out := make([]*packer, 0, len(r.registry))
	for p := range r.registry {
		out = append(out, p)
	}
	return out
}

func (r *packerManager) Flush(ctx context.Context) error {
	r.pm.Lock()
	defer r.pm.Unlock()

	pendingPackers, err := r.mergePackers()
	if err != nil {
		return err
	}

	for _, packer := range pendingPackers {
		debug.Log("manually flushing pending pack")
		err := r.queueFn(ctx, r.tpe, packer)
		if err != nil {
			// keep the handle registered so an error path can still release it
			return err
		}
		// ownership of the handle now moves to the uploader
		r.unregisterPacker(packer)
	}
	return nil
}

// discard closes and removes all open pack handles without uploading them. It
// is used on shutdown or error paths so that no temporary files or open
// descriptors leak. The caller must hold pm.
func (r *packerManager) discard() {
	for _, p := range r.openPackers() {
		r.unregisterPacker(p)
		p.cleanup()
	}
	r.packers = make([]*packer, len(r.packers))
}

// mergePackers merges small pack files before those are uploaded by Flush(). The main
// purpose of this method is to reduce information leaks if a small file is backed up
// and the blobs end up in separate pack files. If the file only consists of two blobs
// this would leak the size of the individual blobs.
func (r *packerManager) mergePackers() ([]*packer, error) {
	pendingPackers := []*packer{}
	var p *packer
	for i, packer := range r.packers {
		if packer == nil {
			continue
		}

		r.packers[i] = nil
		if p == nil {
			p = packer
		} else if p.Size()+packer.Size() < r.packSize {
			// merge if the result stays below the target pack size
			err := packer.bufWr.Flush()
			if err != nil {
				return nil, err
			}
			_, err = packer.tmpfile.Seek(0, io.SeekStart)
			if err != nil {
				return nil, err
			}

			err = p.Merge(packer.Packer, packer.tmpfile)
			if err != nil {
				return nil, err
			}
			// the merged packer no longer exists as a standalone handle
			r.unregisterPacker(packer)
			packer.cleanup()
		} else {
			pendingPackers = append(pendingPackers, p)
			p = packer
		}
	}
	if p != nil {
		pendingPackers = append(pendingPackers, p)
	}
	return pendingPackers, nil
}

func (r *packerManager) SaveBlob(ctx context.Context, t restic.BlobType, id restic.ID, ciphertext []byte, uncompressedLength int) (int, error) {
	r.pm.Lock()
	defer r.pm.Unlock()

	packer, err := r.pickPacker(len(ciphertext))
	if err != nil {
		return 0, err
	}

	// save ciphertext
	// Add only appends bytes in memory to avoid being a scaling bottleneck
	size, err := packer.Add(t, id, ciphertext, uncompressedLength)
	if err != nil {
		return 0, err
	}

	// if the pack and header is not full enough, put back to the list
	if packer.Size() < r.packSize && !packer.HeaderFull() {
		debug.Log("pack is not full enough (%d bytes)", packer.Size())
		return size, nil
	}

	// drop the full packer from the rotation slots. It stays registered until
	// it has been handed to the uploader, so a failed queue call does not leave
	// the handle untracked.
	r.detachPacker(packer)

	// call while holding lock to prevent findPacker from creating new packers if the uploaders are busy
	// else write the pack to the backend
	err = r.queueFn(ctx, t, packer)
	if err != nil {
		// the packer remains in the registry so discard()/Flush() can still release it
		return 0, err
	}
	// ownership of the handle now moves to the uploader
	r.unregisterPacker(packer)

	return size + packer.HeaderOverhead(), nil
}

func randomInt(max int) (int, error) {
	rangeSize := big.NewInt(int64(max))
	randomInt, err := rand.Int(rand.Reader, rangeSize)
	if err != nil {
		return 0, err
	}
	return int(randomInt.Int64()), nil
}

// pickPacker returns or creates a randomly selected packer into which the blob should be stored. If the
// ciphertext is larger than the packSize, a new packer is returned.
func (r *packerManager) pickPacker(ciphertextLen int) (*packer, error) {
	// use separate packer if compressed length is larger than the packsize
	// this speeds up the garbage collection of oversized blobs and reduces the cache size
	// as the oversize blobs are only downloaded if necessary
	if ciphertextLen >= int(r.packSize) {
		return r.newPacker()
	}

	// randomly distribute blobs onto multiple packer instances. This makes it harder for
	// an attacker to learn at which points a file was chunked and therefore mitigates the attack described in
	// https://www.daemonology.net/blog/chunking-attacks.pdf .
	// See https://github.com/restic/restic/issues/5291#issuecomment-2746146193 for details on the mitigation.
	idx, err := randomInt(len(r.packers))
	if err != nil {
		return nil, err
	}

	// retrieve packer or get a new one
	packer := r.packers[idx]
	if packer == nil {
		packer, err = r.newPacker()
		if err != nil {
			return nil, err
		}
		r.packers[idx] = packer
	}
	return packer, nil
}

// detachPacker drops the given packer from the rotation slots but keeps it in
// the handle registry. This is used when a pack becomes full: it must not be
// selected for further blobs, yet the manager still owns the handle until the
// uploader has accepted it.
func (r *packerManager) detachPacker(packer *packer) {
	for i, p := range r.packers {
		if packer == p {
			r.packers[i] = nil
		}
	}
}

// findPacker returns a packer for a new blob of size bytes. Either a new one is
// created or one is returned that already has some blobs.
func (r *packerManager) newPacker() (pck *packer, err error) {
	debug.Log("create new pack")
	tmpfile, err := fileio.TempFile("", "restic-temp-pack-")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	bufWr := bufio.NewWriter(tmpfile)
	p := pack.NewPacker(r.key, bufWr)
	pck = &packer{
		Packer:  p,
		tmpfile: tmpfile,
		bufWr:   bufWr,
	}
	// register the new handle while pm is held
	r.registerPacker(pck)

	return pck, nil
}

// savePacker stores p in the backend. The operation is idempotent: if the pack
// has already been uploaded (for example because a previous attempt failed only
// after the backend stored it), it is not uploaded again. This makes it safe to
// retry a failed upload without leaving duplicate or orphaned packs behind.
func (r *Repository) savePacker(ctx context.Context, t restic.BlobType, p *packer) error {
	debug.Log("save packer for %v with %d blobs (%d bytes)\n", t, p.Packer.Count(), p.Packer.Size())

	id, err := p.finalizePack()
	if err != nil {
		return err
	}

	h := backend.Handle{Type: backend.PackFile, Name: id.String(), IsMetadata: t.IsMetadata()}

	if !p.uploaded {
		// check whether a previous attempt already stored the pack before uploading
		if fi, statErr := r.be.Stat(ctx, h); statErr == nil {
			debug.Log("pack %v already exists (%d bytes), skipping upload", h, fi.Size)
			p.uploaded = true
		} else if !r.be.IsNotExist(statErr) {
			return statErr
		}
	}

	if !p.uploaded {
		// rewind the temp file: finalizePack() left the read position at EOF and
		// a failed Save may have stopped anywhere in between.
		if _, err = p.tmpfile.Seek(0, io.SeekStart); err != nil {
			return err
		}

		// first pass: let the backend hasher accumulate over the pack contents
		var rd io.Reader = p.tmpfile
		var beHr *hashing.Reader
		if beHasher := r.be.Hasher(); beHasher != nil {
			beHr = hashing.NewReader(rd, beHasher)
			rd = beHr
		}
		if _, err = io.Copy(io.Discard, rd); err != nil {
			return err
		}

		// NewFileReader seeks to EOF (to get the length) and rewinds afterwards
		rrd, err := backend.NewFileReader(p.tmpfile, beHash(beHr))
		if err != nil {
			return err
		}

		err = r.be.Save(ctx, h, rrd)
		if err != nil {
			debug.Log("Save(%v) error: %v", h, err)
			return err
		}
		p.uploaded = true
		debug.Log("saved as %v", h)
	}

	p.cleanup()

	// update blobs in the index
	debug.Log("  updating blobs %v to pack %v", p.Packer.Blobs(), id)
	return r.idx.StorePack(ctx, id, p.Packer.Blobs(), &internalRepository{r})
}

// beHash returns the hash accumulated by the reader, or nil if the backend
// does not require one.
func beHash(hr *hashing.Reader) []byte {
	if hr == nil {
		return nil
	}
	return hr.Sum(nil)
}
