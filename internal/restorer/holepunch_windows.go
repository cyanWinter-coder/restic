package restorer

import (
	"os"
	"unsafe"

	"github.com/restic/restic/internal/debug"
	"golang.org/x/sys/windows"
)

// ensureSparse marks the file as sparse if possible. Errors are only logged,
// callers must be able to fall back to writing zeros explicitly.
func ensureSparse(f *os.File) error {
	var bytesReturned uint32
	err := windows.DeviceIoControl(windows.Handle(f.Fd()), windows.FSCTL_SET_SPARSE, nil, 0, nil, 0, &bytesReturned, nil)
	if err != nil {
		debug.Log("failed to set sparse attribute for %v: %v", f.Name(), err)
	}
	return err
}

// punchHole deallocates the file range [offset, offset+length), so that
// subsequent reads return zeros without consuming disk blocks. The file must
// have been marked sparse beforehand. If hole punching is not supported, the
// error is returned and the caller falls back to writing zeros explicitly.
func punchHole(f *os.File, offset, length int64) error {
	if length <= 0 {
		return nil
	}

	// FSCTL_SET_ZERO_DATA only has an effect on sparse files. Make sure the
	// sparse flag is set; ignore failures here as DeviceIoControl below will
	// report the actual error.
	_ = ensureSparse(f)

	in := windows.FileZeroDataInformation{
		FileOffset:      offset,
		BeyondFinalZero: offset + length,
	}
	var bytesReturned uint32
	return windows.DeviceIoControl(
		windows.Handle(f.Fd()),
		windows.FSCTL_SET_ZERO_DATA,
		(*byte)(unsafe.Pointer(&in)),
		uint32(unsafe.Sizeof(in)),
		nil, 0,
		&bytesReturned,
		nil,
	)
}
