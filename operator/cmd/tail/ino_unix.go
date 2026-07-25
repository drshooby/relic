//go:build unix

package main

import (
	"io/fs"
	"syscall"
)

// ino returns the inode number, which identifies the file independently of its
// path. A change means something replaced the file we were reading.
func ino(fi fs.FileInfo) uint64 {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(st.Ino)
}
