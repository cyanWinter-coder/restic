//go:build linux

package restorer

import (
	"os"

	"golang.org/x/sys/unix"
)

// punchHole deallocates the file range [offset, offset+length), so that
// subsequent reads return zeros without consuming disk blocks. If the
// filesystem does not support hole punching, the error is returned and the
// caller falls back to writing zeros explicitly.
func punchHole(f *os.File, offset, length int64) error {
	if length <= 0 {
		return nil
	}
	return unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, offset, length)
}
