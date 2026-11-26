//go:build !linux && !windows

package restorer

import (
	"os"

	"github.com/restic/restic/internal/errors"
)

// errHolePunchUnsupported is returned on platforms that have no API to
// deallocate file ranges. Callers treat it as a non-fatal hint and keep the
// sparse sections as explicitly written zeros.
var errHolePunchUnsupported = errors.New("hole punching is not supported on this platform")

// punchHole deallocates the file range [offset, offset+length), so that
// subsequent reads return zeros without consuming disk blocks.
func punchHole(f *os.File, offset, length int64) error {
	return errHolePunchUnsupported
}
