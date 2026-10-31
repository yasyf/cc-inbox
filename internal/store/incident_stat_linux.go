package store

import "syscall"

func incidentChangeTime(stat *syscall.Stat_t) int64 {
	return stat.Ctim.Nano()
}
