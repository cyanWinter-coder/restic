package fs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHandlePoolQueuesAndReleases(t *testing.T) {
	p := NewHandlePool(1)
	ctx := context.Background()

	if err := p.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if p.TryAcquire() {
		t.Fatal("second acquisition should not succeed while the only slot is held")
	}

	acquired := make(chan struct{})
	go func() {
		if err := p.Acquire(ctx); err != nil {
			t.Error(err)
			return
		}
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("queued acquisition completed before Release")
	case <-time.After(20 * time.Millisecond):
	}

	p.Release()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("queued acquisition did not complete after Release")
	}
}

func TestHandlePoolAcquireCanceledWhileQueued(t *testing.T) {
	p := NewHandlePool(1)
	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer p.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.Acquire(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPooledFileReleasesOnce(t *testing.T) {
	p := NewHandlePool(1)
	if !p.TryAcquire() {
		t.Fatal("TryAcquire failed")
	}

	base := &closeCountFile{}
	f := p.OnClose(base)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if base.closes != 2 {
		t.Fatalf("underlying Close calls = %d, want 2", base.closes)
	}

	done := make(chan struct{})
	go func() { p.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pool Wait did not return after pooled file close")
	}
}

type closeCountFile struct {
	File
	closes int
}

func (f *closeCountFile) Close() error {
	f.closes++
	return nil
}
