package shm

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

// OpenOrCreateMmap creates or opens a file and memory-maps it.
func OpenOrCreateMmap(path string, size int) (mmap.MMap, *os.File, error) {
	// Open file, create if not exists.
	// We use 0600 (owner read/write only) for maximum security.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, err
	}

	// Ensure file is of correct size
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if stat.Size() < int64(size) {
		if err := f.Truncate(int64(size)); err != nil {
			f.Close()
			return nil, nil, err
		}
	}

	// Map the file
	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	return m, f, nil
}
