package fs

import (
	"context"
	"sync"
)

// HandlePool bounds the number of concurrently used file handles.
//
// Acquire queues when the limit has been reached. A slot is returned with
// Release, or automatically when a File returned by OnClose is closed.
type HandlePool struct {
	tokens chan struct{}

	wg sync.WaitGroup
}

// NewHandlePool creates a pool that allows at most limit handles. A limit of
// zero or less means that no limit is enforced.
func NewHandlePool(limit int) *HandlePool {
	p := &HandlePool{}
	if limit > 0 {
		p.tokens = make(chan struct{}, limit)
	}
	return p
}

// Limit returns the configured handle limit. It returns zero when unlimited.
func (p *HandlePool) Limit() int {
	if p == nil || p.tokens == nil {
		return 0
	}
	return cap(p.tokens)
}

// Acquire reserves one handle slot, waiting if necessary. It returns the
// context error if ctx is canceled before a slot becomes available.
func (p *HandlePool) Acquire(ctx context.Context) error {
	if p == nil || p.tokens == nil {
		return nil
	}

	select {
	case p.tokens <- struct{}{}:
		p.wg.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire reserves a slot only if one is immediately available.
func (p *HandlePool) TryAcquire() bool {
	if p == nil || p.tokens == nil {
		return true
	}

	select {
	case p.tokens <- struct{}{}:
		p.wg.Add(1)
		return true
	default:
		return false
	}
}

// Release returns one reserved slot to the pool.
func (p *HandlePool) Release() {
	if p == nil || p.tokens == nil {
		return
	}

	select {
	case <-p.tokens:
		p.wg.Done()
	default:
		// A Release without a matching Acquire is a programming error. Keep
		// the pool usable in production builds rather than panicking, but make
		// the mismatch visible through the wait group invariant in tests.
		panic("fs: release of unacquired handle pool slot")
	}
}

// Wait blocks until all acquired slots have been released.
func (p *HandlePool) Wait() {
	if p == nil {
		return
	}
	p.wg.Wait()
}

// OnClose wraps f so that the first Close releases one pool slot.
func (p *HandlePool) OnClose(f File) File {
	if p == nil || p.tokens == nil {
		return f
	}
	return &pooledFile{File: f, pool: p}
}

type pooledFile struct {
	File
	pool *HandlePool
	once sync.Once
}

func (f *pooledFile) Close() error {
	err := f.File.Close()
	f.once.Do(f.pool.Release)
	return err
}
