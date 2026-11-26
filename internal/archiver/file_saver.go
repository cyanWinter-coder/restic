package archiver

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

const chunkReadBufSize = 512 * 1024 // matches chunker internal read buffer size

// fileSaver concurrently saves incoming files to the repo.
type fileSaver struct {
	uploader restic.BlobSaverAsync

	chunkerFactory restic.ChunkerFactory

	ch chan<- saveFileJob

	// filesys is used for a final path-based stat after reading. When nil,
	// the open file description is stat'ed instead.
	filesys fs.FS

	// onReadChange is invoked for a size/mtime mismatch after reading.
	onReadChange func(context.Context, ReadChange)

	readBlockSize int

	CompleteBlob func(bytes uint64)

	NodeFromFileInfo func(snPath, filename string, meta toNoder, ignoreXattrListError bool) (*data.Node, error)
}

// newFileSaver returns a new file saver. A worker pool with fileWorkers is
// started, it is stopped when ctx is cancelled.
func newFileSaver(ctx context.Context, wg *errgroup.Group, uploader restic.BlobSaverAsync, chunkerFactory restic.ChunkerFactory, fileWorkers uint) *fileSaver {
	return newFileSaverWithBlockSize(ctx, wg, uploader, chunkerFactory, fileWorkers, chunkReadBufSize)
}

func newFileSaverWithBlockSize(ctx context.Context, wg *errgroup.Group, uploader restic.BlobSaverAsync, chunkerFactory restic.ChunkerFactory, fileWorkers uint, readBlockSize int) *fileSaver {
	ch := make(chan saveFileJob)
	debug.Log("new file saver with %v file workers", fileWorkers)

	if readBlockSize <= 0 {
		readBlockSize = chunkReadBufSize
	}
	s := &fileSaver{
		uploader:       uploader,
		chunkerFactory: chunkerFactory,
		ch:             ch,
		readBlockSize:  readBlockSize,

		CompleteBlob: func(uint64) {},
	}

	for range fileWorkers {
		wg.Go(func() error {
			s.worker(ctx, ch)
			return nil
		})
	}

	return s
}

func (s *fileSaver) TriggerShutdown() {
	close(s.ch)
}

// fileCompleteFunc is called when the file has been saved.
type fileCompleteFunc func(*data.Node, ItemStats)

// Save stores the file f and returns the data once it has been completed. The
// file is closed by Save. completeReading is only called if the file was read
// successfully. complete is always called. If completeReading is called, then
// this will always happen before calling complete. The callbacks must not block.
func (s *fileSaver) Save(ctx context.Context, snPath string, target string, file fs.File, start func(), completeReading func(), complete fileCompleteFunc) futureNode {
	fn, ch := newFutureNode()
	job := saveFileJob{
		snPath: snPath,
		target: target,
		file:   file,
		ch:     ch,

		start:           start,
		completeReading: completeReading,
		complete:        complete,
	}

	select {
	case s.ch <- job:
	case <-ctx.Done():
		debug.Log("not sending job, context is cancelled: %v", ctx.Err())
		_ = file.Close()
		close(ch)
	}

	return fn
}

type saveFileJob struct {
	snPath string
	target string
	file   fs.File
	ch     chan<- futureNodeResult

	start           func()
	completeReading func()
	complete        fileCompleteFunc
}

type fileChunkState struct {
	readBuf []byte
	bpos    uint
	bmax    uint
	closed  bool
}

func (s *fileChunkState) reset() {
	s.bpos = 0
	s.bmax = 0
	s.closed = false
}

// readNextChunk reads from rd and returns the next chunk of data. io.EOF is
// returned when all chunks have been read.
func (s *fileChunkState) readNextChunk(rd io.Reader, chnker restic.Chunker, data []byte) ([]byte, error) {
	data = data[:0]
	for {
		if s.bpos >= s.bmax {
			n, err := io.ReadFull(rd, s.readBuf)

			if err == io.ErrUnexpectedEOF {
				err = nil
			}

			// io.EOF only happens when the end of the file has been reached.
			// If this is the case, we need to return the data we have read so far.
			if err == io.EOF && !s.closed {
				s.closed = true

				if len(data) > 0 {
					return data, nil
				}
			}

			if err != nil {
				return nil, err
			}

			s.bpos = 0
			s.bmax = uint(n)
		}

		split := chnker.NextSplitPoint(s.readBuf[s.bpos:s.bmax])
		if split == -1 {
			data = append(data, s.readBuf[s.bpos:s.bmax]...)
			s.bpos = s.bmax
		} else {
			data = append(data, s.readBuf[s.bpos:s.bpos+uint(split)]...)
			s.bpos += uint(split)
			return data, nil
		}
	}
}

// saveFile stores the file f in the repo, then closes it.
func (s *fileSaver) saveFile(ctx context.Context, chnker restic.Chunker, chunkState *fileChunkState, snPath string, target string, f fs.File, start func(), finishReading func(), finish func(res futureNodeResult)) {
	start()

	fnr := futureNodeResult{
		snPath: snPath,
		target: target,
	}
	var lock sync.Mutex
	remaining := 0
	isCompleted := false

	completeBlob := func() {
		lock.Lock()
		defer lock.Unlock()

		remaining--
		if remaining == 0 && fnr.err == nil {
			if isCompleted {
				panic("completed twice")
			}
			for _, id := range fnr.node.Content {
				if id.IsNull() {
					panic("completed file with null ID")
				}
			}
			isCompleted = true
			finish(fnr)
		}
	}
	completeError := func(err error) {
		lock.Lock()
		defer lock.Unlock()

		if fnr.err == nil {
			if isCompleted {
				panic("completed twice")
			}
			isCompleted = true
			fnr.err = fmt.Errorf("failed to save %v: %w", target, err)
			fnr.node = nil
			fnr.stats = ItemStats{}
			finish(fnr)
		}
	}

	debug.Log("%v", snPath)

	// Snapshot size and mtime immediately before reading. The matching
	// post-read check catches concurrent rewrites that would otherwise produce
	// an internally inconsistent backup without any diagnostic.
	beforeFI, err := f.Stat()
	if err != nil {
		_ = f.Close()
		completeError(err)
		return
	}

	node, err := s.NodeFromFileInfo(snPath, target, f, false)
	if err != nil {
		_ = f.Close()
		completeError(err)
		return
	}

	if node.Type != data.NodeTypeFile {
		_ = f.Close()
		completeError(errors.Errorf("node type %q is wrong", node.Type))
		return
	}

	chnker.Reset()
	chunkState.reset()

	node.Content = []restic.ID{}
	node.Size = 0
	var idx int
	for {
		buf := s.uploader.BlobBufferPool().Get()
		chunkData, err := chunkState.readNextChunk(f, chnker, buf.Data)
		if err == io.EOF {
			buf.Release()
			break
		}
		if err != nil {
			buf.Release()
			_ = f.Close()
			completeError(err)
			return
		}

		// put result buffer back for later reuse
		buf.Data = chunkData
		chunkSize := len(buf.Data)
		node.Size += uint64(chunkSize)

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			buf.Release()
			_ = f.Close()
			completeError(ctx.Err())
			return
		}

		// add a place to store the saveBlob result
		pos := idx

		lock.Lock()
		node.Content = append(node.Content, restic.ID{})
		lock.Unlock()

		s.uploader.SaveBlobAsync(ctx, restic.DataBlob, buf, restic.ID{}, false, func(newID restic.ID, known bool, sizeInRepo int, err error) {
			if err != nil {
				completeError(err)
				return
			}

			lock.Lock()
			if !known {
				fnr.stats.DataBlobs++
				fnr.stats.DataSize += uint64(chunkSize)
				fnr.stats.DataSizeInRepo += uint64(sizeInRepo)
			}
			node.Content[pos] = newID
			lock.Unlock()

			completeBlob()
		})
		idx++

		// test if the context has been cancelled, return the error
		if ctx.Err() != nil {
			_ = f.Close()
			completeError(ctx.Err())
			return
		}

		s.CompleteBlob(uint64(chunkSize))
	}

	if s.onReadChange != nil {
		if change := detectReadChange(target, f, beforeFI, s.filesys); change != nil {
			s.onReadChange(ctx, *change)
		}
	}

	err = f.Close()
	if err != nil {
		completeError(err)
		return
	}

	fnr.node = node
	lock.Lock()
	// require one additional completeFuture() call to ensure that the future only completes
	// after reaching the end of this method
	remaining += idx + 1
	lock.Unlock()
	finishReading()
	completeBlob()
}

func detectReadChange(target string, f fs.File, before *fs.ExtendedFileInfo, pathFS fs.FS) *ReadChange {
	if before == nil {
		return nil
	}

	change := &ReadChange{
		Path:       target,
		Before:     fileStateFromFI(before),
		DetectedAt: time.Now(),
	}

	changed := false

	// Stat the open description first. Some fs.File implementations cache the
	// FileInfo from before reading, so the path Lstat below is the authoritative
	// post-read comparison and also detects replacement via rename.
	if afterHandle, err := f.Stat(); err == nil && afterHandle != nil {
		if before.Size != afterHandle.Size || !before.ModTime.Equal(afterHandle.ModTime) {
			changed = true
			change.After = fileStateFromFI(afterHandle)
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		change.StatError = err.Error()
		changed = true
	}

	// A path stat additionally detects replacement via rename, where the old
	// open description continues to refer to the unlinked original inode.
	if pathFS != nil {
		afterPath, err := pathFS.Lstat(target)
		switch {
		case errors.Is(err, os.ErrNotExist):
			change.Deleted = true
			changed = true
		case err != nil:
			change.StatError = err.Error()
			changed = true
		case afterPath != nil:
			state := fileStateFromFI(afterPath)
			if before.Size != afterPath.Size || !before.ModTime.Equal(afterPath.ModTime) {
				changed = true
			}
			if change.After == (FileState{}) {
				change.After = state
			}
		}
	}

	if !changed {
		return nil
	}
	if change.After == (FileState{}) && !change.Deleted {
		// Keep the before state usable in diagnostics if only stat metadata
		// could not be retrieved.
		change.After = change.Before
	}
	return change
}

func (s *fileSaver) worker(ctx context.Context, jobs <-chan saveFileJob) {
	chnker := s.chunkerFactory.NewChunker()
	blockSize := s.readBlockSize
	if blockSize <= 0 {
		blockSize = chunkReadBufSize
	}
	chunkState := &fileChunkState{readBuf: make([]byte, blockSize)}

	for {
		var job saveFileJob
		var ok bool
		select {
		case <-ctx.Done():
			return
		case job, ok = <-jobs:
			if !ok {
				return
			}
		}

		s.saveFile(ctx, chnker, chunkState, job.snPath, job.target, job.file, job.start, func() {
			if job.completeReading != nil {
				job.completeReading()
			}
		}, func(res futureNodeResult) {
			if job.complete != nil {
				job.complete(res.node, res.stats)
			}
			job.ch <- res
			close(job.ch)
		})
	}
}
