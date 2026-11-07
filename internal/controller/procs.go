package controller

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// proc is a child process started by the controller.
type proc struct {
	name   string
	cmd    *exec.Cmd
	exited chan struct{}
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

func (p *proc) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func (p *proc) signal(sig syscall.Signal) { _ = p.cmd.Process.Signal(sig) }

// stop asks the process to exit and kills it if it does not within the timeout.
func (p *proc) stop(timeout time.Duration) {
	if !p.alive() {
		return
	}
	p.signal(syscall.SIGTERM)
	select {
	case <-p.exited:
	case <-time.After(timeout):
		p.signal(syscall.SIGKILL)
		<-p.exited
	}
}

// procs starts children and remembers their PIDs in a file so a crashed
// controller's leftovers are cleaned up on the next start.
type procs struct {
	runDir string

	mu      sync.Mutex
	running []*proc
}

func newProcs(runDir string) (*procs, error) {
	for _, dir := range []string{"logs", "bin"} {
		if err := os.MkdirAll(filepath.Join(runDir, dir), 0o755); err != nil {
			return nil, err
		}
	}
	p := &procs{runDir: runDir}
	p.killLeftovers()
	return p, nil
}

func (p *procs) pidFile() string { return filepath.Join(p.runDir, "pids") }

// start runs binary with argv[0] set to name, so ps/top/htop show the name.
func (p *procs) start(name, binary, dir string, args ...string) (*proc, error) {
	logFile, err := os.Create(filepath.Join(p.runDir, "logs", name+".log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, args...)
	cmd.Args[0] = name
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = logFile, logFile
	// Own process group: Ctrl-C reaches only the controller, which then stops children in order.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}

	child := &proc{name: name, cmd: cmd, exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		logFile.Close()
		close(child.exited)
		p.forget(child)
	}()

	p.mu.Lock()
	p.running = append(p.running, child)
	p.writePIDsLocked()
	p.mu.Unlock()
	return child, nil
}

// startWorker launches demo-worker-N through a hard link of the worker binary
// named after the worker, because macOS top shows the executable file name.
func (p *procs) startWorker(index int, workerBin string, args ...string) (*proc, error) {
	name := "demo-worker-" + strconv.Itoa(index)
	link := filepath.Join(p.runDir, "bin", name)
	_ = os.Remove(link)
	if err := os.Link(workerBin, link); err != nil {
		return nil, fmt.Errorf("link worker binary: %w", err)
	}
	return p.start(name, link, ".", args...)
}

func (p *procs) forget(child *proc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, c := range p.running {
		if c == child {
			p.running = append(p.running[:i], p.running[i+1:]...)
			break
		}
	}
	p.writePIDsLocked()
}

func (p *procs) writePIDsLocked() {
	var b strings.Builder
	for _, c := range p.running {
		fmt.Fprintf(&b, "%d %s\n", c.pid(), c.name)
	}
	_ = os.WriteFile(p.pidFile(), []byte(b.String()), 0o644)
}

// killLeftovers kills processes recorded by a previous controller run, but
// only if the PID still belongs to a process with the recorded name.
func (p *procs) killLeftovers() {
	data, err := os.ReadFile(p.pidFile())
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		pidStr, name, ok := strings.Cut(line, " ")
		pid, err := strconv.Atoi(pidStr)
		if !ok || err != nil {
			continue
		}
		out, err := exec.Command("ps", "-o", "command=", "-p", pidStr).Output()
		if err == nil && strings.HasPrefix(strings.TrimSpace(string(out)), name) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	_ = os.Remove(p.pidFile())
}

func waitForTCP(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", address, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not reachable after %s: %w", address, timeout, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
