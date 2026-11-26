package restorer

import (
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/fileio"
)

// WriteAt writes p to f.File at offset. For sparse files it keeps every
// maximal all-zero range sparse: leading and trailing zeros and any zero run
// inside p are deallocated (punched as holes) instead of being written as
// data blocks, while non-zero runs are written normally.
//
// If hole punching is not supported by the platform or filesystem, it falls
// back to explicitly writing zeros, so the restored file content is always
// correct even if it ends up fully allocated.
func (f *partialFile) WriteAt(p []byte, offset int64) (n int, err error) {
	if !f.sparse {
		return f.File.WriteAt(p, offset)
	}

	n = len(p)
	base := offset

	for i := 0; i < len(p); {
		// find the next maximal zero run
		j := i
		for j < len(p) && p[j] == 0 {
			j++
		}
		if j > i {
			// [i, j) is all zero: make it a hole
			holeOff := base + int64(i)
			holeLen := int64(j - i)
			if holeErr := punchHole(f.File, holeOff, holeLen); holeErr != nil {
				debug.Log("punching hole in %v at %d failed, falling back to explicit zero write: %v", f.Name(), holeOff, holeErr)
				if err = fileio.WriteZeros(f.File, holeOff, holeLen); err != nil {
					return i, err
				}
			}
			i = j
			continue
		}

		// find the end of the following non-zero run and write it
		j = i
		for j < len(p) && p[j] != 0 {
			j++
		}
		if _, err = f.File.WriteAt(p[i:j], base+int64(i)); err != nil {
			return i, err
		}
		i = j
	}

	return n, nil
}
