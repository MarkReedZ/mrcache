package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestProtocol(t *testing.T) {
	srv := startMrcache(t, "-M", "8388608", "-B", "1048576", "-i", "4", "-p")
	c, r := dial(t, srv.addr)
	defer c.Close()

	if _, err := c.Write(appendSet(nil, []byte("hello"), []byte("world"))); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(appendGet(nil, []byte("hello"))); err != nil {
		t.Fatal(err)
	}
	got, miss, err := readGet(r)
	if err != nil || miss || string(got) != "world" {
		t.Fatalf("get %q miss=%v err=%v", got, miss, err)
	}

	// Empty value is a hit, not a miss.
	if _, err := c.Write(appendSet(nil, []byte("empty"), nil)); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(appendGet(nil, []byte("empty"))); err != nil {
		t.Fatal(err)
	}
	got, miss, err = readGet(r)
	if err != nil || miss || len(got) != 0 {
		t.Fatalf("empty get %q miss=%v err=%v", got, miss, err)
	}

	if _, err := c.Write(appendGet(nil, []byte("missing"))); err != nil {
		t.Fatal(err)
	}
	if _, miss, err = readGet(r); err != nil || !miss {
		t.Fatalf("miss=%v err=%v", miss, err)
	}

	// Overwrite, delete, binary key.
	if _, err := c.Write(appendSet(nil, []byte("hello"), []byte("WORLD"))); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(r); err != nil {
		t.Fatal(err)
	}
	binKey := []byte{0, 1, 2, 255, 0}
	if _, err := c.Write(appendSet(nil, binKey, []byte{9, 9})); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(appendGet(nil, binKey)); err != nil {
		t.Fatal(err)
	}
	got, miss, err = readGet(r)
	if err != nil || miss || len(got) != 2 || got[0] != 9 {
		t.Fatalf("binary %v miss=%v err=%v", got, miss, err)
	}

	if _, err := c.Write(appendDel(nil, []byte("hello"))); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(appendGet(nil, []byte("hello"))); err != nil {
		t.Fatal(err)
	}
	if _, miss, err = readGet(r); err != nil || !miss {
		t.Fatalf("deleted miss=%v err=%v", miss, err)
	}

	if _, err := c.Write(appendPing(nil)); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(appendStat(nil)); err != nil {
		t.Fatal(err)
	}
	st, err := readStat(r)
	if err != nil {
		t.Fatal(err)
	}
	if st.sets < 3 || st.hits < 2 || st.misses < 1 || st.dels < 1 || st.live < 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestPipelineAndSplitWrites(t *testing.T) {
	srv := startMrcache(t, "-M", "8388608", "-B", "1048576", "-i", "4", "-p")
	c, r := dial(t, srv.addr)
	defer c.Close()

	var buf []byte
	for i := 0; i < 100; i++ {
		buf = appendSet(buf, []byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
	}
	if _, err := c.Write(buf); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := readStatus(r); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	buf = buf[:0]
	for i := 0; i < 100; i++ {
		buf = appendGet(buf, []byte(fmt.Sprintf("k%03d", i)))
	}
	// One byte at a time, so commands cross recv boundaries.
	for _, b := range buf {
		if _, err := c.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		got, miss, err := readGet(r)
		if err != nil || miss || string(got) != fmt.Sprintf("v%03d", i) {
			t.Fatalf("i=%d got=%q miss=%v err=%v", i, got, miss, err)
		}
	}
}

func TestEviction(t *testing.T) {
	// Four 8 KiB blocks. 3000-byte values leave room for two items per block.
	srv := startMrcache(t, "-M", "32768", "-B", "8192", "-i", "4", "-p")
	c, r := dial(t, srv.addr)
	defer c.Close()
	val := bytesOf(3000, 'x')
	for i := 0; i < 20; i++ {
		if _, err := c.Write(appendSet(nil, []byte(fmt.Sprintf("k%02d", i)), val)); err != nil {
			t.Fatal(err)
		}
		if err := readStatus(r); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Write(appendGet(nil, []byte("k19"))); err != nil {
		t.Fatal(err)
	}
	got, miss, err := readGet(r)
	if err != nil || miss || len(got) != 3000 {
		t.Fatalf("recent miss=%v len=%d err=%v", miss, len(got), err)
	}
	if _, err := c.Write(appendGet(nil, []byte("k00"))); err != nil {
		t.Fatal(err)
	}
	if _, miss, err = readGet(r); err != nil || !miss {
		t.Fatalf("oldest should be gone miss=%v err=%v", miss, err)
	}
	if _, err := c.Write(appendStat(nil)); err != nil {
		t.Fatal(err)
	}
	st, err := readStat(r)
	if err != nil {
		t.Fatal(err)
	}
	if st.evicts == 0 || st.live >= 20 {
		t.Fatalf("expected eviction %+v", st)
	}
}

func TestTooBig(t *testing.T) {
	srv := startMrcache(t, "-M", "8192", "-B", "4096", "-i", "1", "-p")
	c, r := dial(t, srv.addr)
	defer c.Close()
	val := bytesOf(5000, 'z')
	if _, err := c.Write(appendSet(nil, []byte("big"), val)); err != nil {
		t.Fatal(err)
	}
	st, err := r.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	if st != 2 {
		t.Fatalf("status %d", st)
	}
	if _, err := c.Write(appendGet(nil, []byte("big"))); err != nil {
		t.Fatal(err)
	}
	if _, miss, err := readGet(r); err != nil || !miss {
		t.Fatalf("stored a value that does not fit miss=%v err=%v", miss, err)
	}
}

func TestConcurrent(t *testing.T) {
	srv := startMrcache(t, "-M", "16777216", "-B", "1048576", "-i", "8", "-p")
	errc := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func(g int) {
			c, err := net.Dial("tcp", srv.addr)
			if err != nil {
				errc <- err
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			key := []byte(fmt.Sprintf("g%d", g))
			val := []byte(fmt.Sprintf("val-%d", g))
			for i := 0; i < 200; i++ {
				if _, err := c.Write(appendSet(nil, key, val)); err != nil {
					errc <- err
					return
				}
				if err := readStatus(r); err != nil {
					errc <- err
					return
				}
				if _, err := c.Write(appendGet(nil, key)); err != nil {
					errc <- err
					return
				}
				got, miss, err := readGet(r)
				if err != nil || miss || string(got) != string(val) {
					errc <- fmt.Errorf("g%d got %q miss %v err %v", g, got, miss, err)
					return
				}
			}
			errc <- nil
		}(g)
	}
	for i := 0; i < 8; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestDisconnect(t *testing.T) {
	srv := startMrcache(t, "-M", "8388608", "-B", "1048576", "-i", "4", "-p")
	c, _ := dial(t, srv.addr)
	if _, err := c.Write([]byte{1, 1, 4, 0}); err != nil { // header only
		t.Fatal(err)
	}
	c.Close()

	c, r := dial(t, srv.addr)
	defer c.Close()
	if _, err := c.Write(appendSet(nil, []byte("after"), []byte("close"))); err != nil {
		t.Fatal(err)
	}
	if err := readStatus(r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(appendGet(nil, []byte("after"))); err != nil {
		t.Fatal(err)
	}
	got, miss, err := readGet(r)
	if err != nil || miss || string(got) != "close" {
		t.Fatalf("after disconnect got %q miss=%v err=%v", got, miss, err)
	}
}

func TestRedisProtocol(t *testing.T) {
	if _, err := exec.LookPath("redis-server"); err != nil {
		t.Skip("redis-server not installed")
	}
	port := freePort(t)
	cmd := exec.Command("redis-server", "--port", strconv.Itoa(port), "--bind", "127.0.0.1",
		"--save", "", "--appendonly", "no", "--protected-mode", "no", "--daemonize", "no")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	waitDial(t, addr)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	if _, err := c.Write(appendRESPSet(nil, []byte("rk"), []byte("rv"))); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readRESP(r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(appendRESPGet(nil, []byte("rk"))); err != nil {
		t.Fatal(err)
	}
	got, miss, err := readRESP(r)
	if err != nil || miss || string(got) != "rv" {
		t.Fatalf("redis get %q miss=%v err=%v", got, miss, err)
	}
	if _, err := c.Write(appendRESPGet(nil, []byte("nope"))); err != nil {
		t.Fatal(err)
	}
	if _, miss, err = readRESP(r); err != nil || !miss {
		t.Fatalf("redis miss=%v err=%v", miss, err)
	}
}

type server struct {
	addr string
	cmd  *exec.Cmd
}

func startMrcache(t *testing.T, args ...string) *server {
	t.Helper()
	bin := filepath.Join("..", "mrcache")
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("build mrcache first: %v", err)
	}
	port := freePort(t)
	all := append(append([]string{}, args...), strconv.Itoa(port))
	cmd := exec.Command(bin, all...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	buf := make([]byte, 64)
	if _, err := stdout.Read(buf); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	waitDial(t, addr)
	return &server{addr: addr, cmd: cmd}
}

func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return c, bufio.NewReader(c)
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func waitDial(t *testing.T, addr string) {
	t.Helper()
	var last error
	for i := 0; i < 100; i++ {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dial %s: %v", addr, last)
}
