package restorer

import "os"

func truncateSparse(f *os.File, size int64) error {
	// try setting the sparse file attribute, but ignore the error if it fails
	_ = ensureSparse(f)

	return f.Truncate(size)
}
