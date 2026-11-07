package controller

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// cpuTimes returns the cumulative CPU time (user+system) of each PID as
// reported by the OS. It never asks Shard Manager or the workers.
func cpuTimes(pids []int) map[int]time.Duration {
	if len(pids) == 0 {
		return nil
	}
	if _, err := os.Stat("/proc/self/stat"); err == nil {
		return procCPUTimes(pids)
	}
	return psCPUTimes(pids)
}

// psCPUTimes parses `ps -o pid=,time=`. On macOS the time column is
// [[hh:]mm:]ss.cc, which gives 10ms resolution.
func psCPUTimes(pids []int) map[int]time.Duration {
	list := make([]string, len(pids))
	for i, pid := range pids {
		list[i] = strconv.Itoa(pid)
	}
	out, _ := exec.Command("ps", "-o", "pid=,time=", "-p", strings.Join(list, ",")).Output()

	times := make(map[int]time.Duration, len(pids))
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		var seconds float64
		for _, part := range strings.Split(fields[1], ":") {
			v, _ := strconv.ParseFloat(part, 64)
			seconds = seconds*60 + v
		}
		times[pid] = time.Duration(seconds * float64(time.Second))
	}
	return times
}

// procCPUTimes reads utime+stime from /proc/<pid>/stat (Linux, USER_HZ=100).
func procCPUTimes(pids []int) map[int]time.Duration {
	const userHZ = 100
	times := make(map[int]time.Duration, len(pids))
	for _, pid := range pids {
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err != nil {
			continue
		}
		// The command name may contain spaces; fields after ") " are fixed.
		rest := string(data)
		if i := strings.LastIndex(rest, ") "); i >= 0 {
			rest = rest[i+2:]
		}
		fields := strings.Fields(rest)
		if len(fields) < 13 {
			continue
		}
		utime, _ := strconv.ParseInt(fields[11], 10, 64)
		stime, _ := strconv.ParseInt(fields[12], 10, 64)
		times[pid] = time.Duration(utime+stime) * time.Second / userHZ
	}
	return times
}
