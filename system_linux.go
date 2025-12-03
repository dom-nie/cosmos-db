//go:build linux

package db

import (
	"runtime"
	"syscall"
)

// SystemResources holds detected system resource information.
type SystemResources struct {
	TotalRAM uint64 // Total system RAM in bytes
	NumCPU   int    // Number of logical CPUs
}

// GetSystemResources detects system RAM and CPU count.
// Uses syscall.Sysinfo which is Linux-specific.
func GetSystemResources() SystemResources {
	var info syscall.Sysinfo_t
	var totalRAM uint64
	if err := syscall.Sysinfo(&info); err == nil {
		// Totalram is in units of info.Unit bytes
		totalRAM = uint64(info.Totalram) * uint64(info.Unit)
	}
	return SystemResources{
		TotalRAM: totalRAM,
		NumCPU:   runtime.NumCPU(),
	}
}
