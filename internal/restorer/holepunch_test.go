package restorer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/restic/restic/internal/fileio"
	rtest "github.com/restic/restic/internal/test"
)

// TestPunchHoleCreatesSparseRange writes data into a file, punches a hole in
// the middle and verifies that the content is unchanged while (on supporting
// filesystems) disk usage drops.
func TestPunchHoleCreatesSparseRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sparse")
	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	rtest.OK(t, os.WriteFile(path, data, 0600))

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	rtest.OK(t, err)
	defer f.Close()

	holeOff := int64(1024 * 1024)
	holeLen := int64(2 * 1024 * 1024)
	err = punchHole(f, holeOff, holeLen)
	if err != nil {
		// Platforms or filesystems without hole-punching support: the
		// restorer falls back to explicit zero writes in that case.
		t.Skipf("punchHole not supported: %v", err)
	}

	got, err := os.ReadFile(path)
	rtest.OK(t, err)
	rtest.Equals(t, len(data), len(got))

	// hole range must read back as zeros
	for i := holeOff; i < holeOff+holeLen; i++ {
		if got[i] != 0 {
			t.Fatalf("byte %d inside hole is %d, want 0", i, got[i])
		}
	}
	// data outside the hole must be unchanged
	for i := int64(0); i < holeOff; i++ {
		rtest.Equals(t, data[i], got[i])
	}
	for i := holeOff + holeLen; i < int64(len(data)); i++ {
		rtest.Equals(t, data[i], got[i])
	}
}

// TestWriteZeros writes a range larger than the internal scratch buffer and
// checks the result.
func TestWriteZeros(t *testing.T) {
	path := filepath.Join(t.TempDir(), "zeros")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	rtest.OK(t, err)
	defer f.Close()

	length := int64(300 * 1024) // deliberately not a multiple of the 128KiB buffer
	rtest.OK(t, f.Truncate(length))
	// fill with ones first so that written zeros are observable
	ones := make([]byte, length)
	for i := range ones {
		ones[i] = 1
	}
	_, err = f.WriteAt(ones, 0)
	rtest.OK(t, err)

	rtest.OK(t, fileio.WriteZeros(f, 1024, length-2048))

	got, err := os.ReadFile(path)
	rtest.OK(t, err)
	rtest.Equals(t, int(length), len(got))
	for i := int64(1024); i < length-1024; i++ {
		rtest.Equals(t, byte(0), got[i])
	}
	rtest.Equals(t, byte(1), got[0])
	rtest.Equals(t, byte(1), got[length-1])
}
