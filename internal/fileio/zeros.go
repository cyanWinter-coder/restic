package fileio

import "os"

// zeroBuf is reused by WriteZeros to avoid allocating a multi-megabyte slice
// for every all-zero chunk that cannot be turned into a sparse hole.
var zeroBuf = make([]byte, 128*1024)

// WriteZeros writes length zero bytes at offset using repeated fixed-size
// writes. It is the fallback used when hole punching is unavailable.
func WriteZeros(f *os.File, offset, length int64) error {
	for length > 0 {
		n := int64(len(zeroBuf))
		if n > length {
			n = length
		}
		if _, err := f.WriteAt(zeroBuf[:n], offset); err != nil {
			return err
		}
		offset += n
		length -= n
	}
	return nil
}
