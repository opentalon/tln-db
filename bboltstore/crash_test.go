package bboltstore_test

// TestSIGKILLDurability verifies that successful Put calls survive an
// unclean process exit. The child process:
//
//  1. Opens a bbolt store at a parent-supplied path.
//  2. Writes a fixed number of documents via individual Put calls,
//     each of which must return nil before the next runs. Because
//     bbolt fsyncs the file on every Update transaction commit, each
//     successful Put is durable on disk as soon as it returns.
//  3. Prints "READY\n" so the parent knows every Put has returned.
//  4. Sleeps long enough for the parent to send SIGKILL.
//
// The parent then reopens the file and Scans tenant-a. The contract:
// every doc the child reported writing must be readable, and no
// torn / partial documents may surface.
//
// What this test does NOT exercise: partial-transaction rollback after
// SIGKILL mid-Update. To trigger that we'd need a kill hook inside the
// bbolt transaction, which would require production-code changes purely
// for testability. bbolt's own internal suite already covers torn-write
// recovery, so we rely on that for the within-transaction case and
// focus here on the cross-transaction durability boundary we own.

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/opentalon/tln-db/bboltstore"
)

const (
	crashChildEnv  = "TLNDB_CRASH_CHILD"
	crashPathEnv   = "TLNDB_CRASH_PATH"
	crashDocCount  = 50
	crashTenantID  = "tenant-crash"
)

func TestMain(m *testing.M) {
	if os.Getenv(crashChildEnv) == "1" {
		runCrashChild()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runCrashChild() {
	path := os.Getenv(crashPathEnv)
	if path == "" {
		fmt.Fprintln(os.Stderr, "child: missing", crashPathEnv)
		os.Exit(2)
	}
	s, err := bboltstore.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "child Open:", err)
		os.Exit(3)
	}
	ctx := context.Background()
	for i := 0; i < crashDocCount; i++ {
		docID := fmt.Sprintf("doc-%03d", i)
		body := fmt.Sprintf(`{"i":%d}`, i)
		if err := s.Put(ctx, crashTenantID, docID, []byte(body)); err != nil {
			fmt.Fprintln(os.Stderr, "child Put:", err)
			os.Exit(4)
		}
	}
	// Intentionally do NOT close; we want SIGKILL to land while the
	// file lock is still held, simulating a real crash.
	fmt.Println("READY")
	// Long enough for the parent to send SIGKILL; if the parent fails
	// to kill us, exit cleanly so the test doesn't hang indefinitely.
	time.Sleep(30 * time.Second)
}

func TestSIGKILLDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess crash test in short mode")
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL semantics not portable to Windows")
	}

	path := filepath.Join(t.TempDir(), "crash.db")

	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		crashChildEnv+"=1",
		crashPathEnv+"="+path,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	scanner := bufio.NewScanner(stdout)
	ready := make(chan string, 1)
	go func() {
		if scanner.Scan() {
			ready <- scanner.Text()
		}
		close(ready)
	}()
	select {
	case line, ok := <-ready:
		if !ok || line != "READY" {
			t.Fatalf("child did not signal READY (got %q)", line)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for child READY")
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	_ = cmd.Wait()

	// Reopen and verify every doc the child reported writing is here.
	s, err := bboltstore.Open(path)
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	count := 0
	seen := make(map[string]bool, crashDocCount)
	if err := s.Scan(context.Background(), crashTenantID, func(id string, doc []byte) bool {
		count++
		seen[id] = true
		return true
	}); err != nil {
		t.Fatalf("Scan after crash: %v", err)
	}
	if count != crashDocCount {
		t.Fatalf("after SIGKILL: found %d docs, want %d", count, crashDocCount)
	}
	for i := 0; i < crashDocCount; i++ {
		want := fmt.Sprintf("doc-%03d", i)
		if !seen[want] {
			t.Errorf("missing doc %q after crash", want)
		}
	}
}
