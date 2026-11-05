package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	opGet  = 1
	opSet  = 2
	opDel  = 3
	opPing = 4
	opStat = 5
)

func main() {
	var (
		port  = flag.Int("p", 7000, "port")
		host  = flag.String("h", "127.0.0.1", "hostname")
		conns = flag.Int("c", 16, "connections")
		secs  = flag.Int("d", 3, "seconds")
		klen  = flag.Int("k", 16, "key length")
		vlen  = flag.Int("v", 128, "value length")
		batch = flag.Int("b", 32, "pipelined commands per round trip")
		keys  = flag.Int("keys", 1, "distinct keys (cycled)")
		op    = flag.String("op", "get", "get or set")
		redis = flag.Bool("redis", false, "speak the Redis protocol")
	)
	flag.Parse()
	if *batch < 1 || *keys < 1 || *klen < 1 || *vlen < 0 {
		fmt.Fprintln(os.Stderr, "invalid sizes")
		os.Exit(2)
	}
	if *redis && *port == 7000 {
		*port = 6379
	}
	addr := fmt.Sprintf("%s:%d", *host, *port)
	keysB := makeKeys(*keys, *klen)
	val := bytesOf(*vlen, 'v')

	if err := preload(addr, keysB, val, *redis, *op); err != nil {
		fmt.Fprintf(os.Stderr, "preload: %v\n", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	type result struct {
		ops int
		rtt time.Duration
		n   int
		err error
	}
	ch := make(chan result, *conns)
	deadline := time.Now().Add(time.Duration(*secs) * time.Second)
	start := time.Now()
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ops, rtt, n, err := worker(addr, keysB, val, *batch, *op, *redis, deadline, id)
			ch <- result{ops, rtt, n, err}
		}(i)
	}
	wg.Wait()
	close(ch)
	elapsed := time.Since(start)

	var ops int
	var rtt time.Duration
	var rounds int
	var failed int
	for r := range ch {
		ops += r.ops
		rtt += r.rtt
		rounds += r.n
		if r.err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "worker: %v\n", r.err)
		}
	}
	if failed > 0 {
		os.Exit(1)
	}
	rate := float64(ops) / elapsed.Seconds()
	us := 0.0
	if rounds > 0 && *batch > 0 {
		us = float64(rtt.Nanoseconds()) / float64(rounds) / float64(*batch) / 1000.0
	}
	fmt.Printf("%s %s  %s ops/sec   mean pipeline latency %.1fµs/op   conns=%d batch=%d keys=%d key=%dB val=%dB\n",
		map[bool]string{true: "redis", false: "mrcache"}[*redis], *op,
		formatInt(int64(rate)), us,
		*conns, *batch, *keys, *klen, *vlen)
}

func worker(addr string, keys [][]byte, val []byte, batch int, op string, redis bool, deadline time.Time, id int) (int, time.Duration, int, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, 0, 0, err
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	defer c.Close()
	r := bufio.NewReaderSize(c, 256*1024)
	got := make([]byte, len(val))
	var buf []byte
	ops, rounds := 0, 0
	var rtt time.Duration
	keyN := len(keys)
	for time.Now().Before(deadline) {
		buf = buf[:0]
		base := (id + rounds) % keyN
		for i := 0; i < batch; i++ {
			k := keys[(base+i)%keyN]
			if redis {
				if op == "set" {
					buf = appendRESPSet(buf, k, val)
				} else {
					buf = appendRESPGet(buf, k)
				}
			} else if op == "set" {
				buf = appendSet(buf, k, val)
			} else {
				buf = appendGet(buf, k)
			}
		}
		t0 := time.Now()
		if _, err = c.Write(buf); err != nil {
			return ops, rtt, rounds, err
		}
		for i := 0; i < batch; i++ {
			k := keys[(base+i)%keyN]
			if redis {
				n, miss, err := readRESPInto(r, got)
				if err != nil {
					return ops, rtt, rounds, err
				}
				if op == "get" && (miss || n != len(val) || !bytesEqual(got[:n], val)) {
					return ops, rtt, rounds, fmt.Errorf("redis get mismatch key %q", k)
				}
			} else if op == "set" {
				if err = readStatus(r); err != nil {
					return ops, rtt, rounds, err
				}
			} else {
				n, miss, err := readGetInto(r, got)
				if err != nil {
					return ops, rtt, rounds, err
				}
				if miss || n != len(val) || !bytesEqual(got[:n], val) {
					return ops, rtt, rounds, fmt.Errorf("get mismatch (%d bytes, miss=%v)", n, miss)
				}
			}
		}
		rtt += time.Since(t0)
		ops += batch
		rounds++
	}
	return ops, rtt, rounds, nil
}

func preload(addr string, keys [][]byte, val []byte, redis bool, op string) error {
	if op == "set" {
		return nil
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	r := bufio.NewReader(c)
	var buf []byte
	const step = 64
	for off := 0; off < len(keys); off += step {
		buf = buf[:0]
		end := off + step
		if end > len(keys) {
			end = len(keys)
		}
		for _, k := range keys[off:end] {
			if redis {
				buf = appendRESPSet(buf, k, val)
			} else {
				buf = appendSet(buf, k, val)
			}
		}
		if _, err = c.Write(buf); err != nil {
			return err
		}
		for range keys[off:end] {
			if redis {
				if _, _, err = readRESP(r); err != nil {
					return err
				}
			} else if err = readStatus(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func makeKeys(n, klen int) [][]byte {
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := bytesOf(klen, 'k')
		s := strconv.Itoa(i)
		if len(s) > len(k) {
			s = s[len(s)-len(k):]
		}
		copy(k[len(k)-len(s):], s)
		out[i] = k
	}
	return out
}

func bytesOf(n int, b byte) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = b
	}
	return p
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func appendGet(dst, key []byte) []byte {
	var hdr [8]byte
	hdr[0] = 1
	hdr[1] = opGet
	binary.LittleEndian.PutUint16(hdr[2:], uint16(len(key)))
	dst = append(dst, hdr[:]...)
	return append(dst, key...)
}

func appendSet(dst, key, val []byte) []byte {
	var hdr [8]byte
	hdr[0] = 1
	hdr[1] = opSet
	binary.LittleEndian.PutUint16(hdr[2:], uint16(len(key)))
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(val)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, key...)
	return append(dst, val...)
}

func appendDel(dst, key []byte) []byte {
	var hdr [8]byte
	hdr[0] = 1
	hdr[1] = opDel
	binary.LittleEndian.PutUint16(hdr[2:], uint16(len(key)))
	dst = append(dst, hdr[:]...)
	return append(dst, key...)
}

func appendPing(dst []byte) []byte {
	return append(dst, 1, opPing, 0, 0, 0, 0, 0, 0)
}

func appendStat(dst []byte) []byte {
	return append(dst, 1, opStat, 0, 0, 0, 0, 0, 0)
}

func readStatus(r *bufio.Reader) error {
	st, err := r.ReadByte()
	if err != nil {
		return err
	}
	if st != 0 && st != 1 && st != 2 {
		return fmt.Errorf("status %d", st)
	}
	return nil
}

func readGetInto(r *bufio.Reader, dst []byte) (n int, miss bool, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, false, err
	}
	vlen := binary.LittleEndian.Uint32(hdr[1:])
	if hdr[0] == 1 {
		if vlen != 0 {
			return 0, false, fmt.Errorf("miss with length %d", vlen)
		}
		return 0, true, nil
	}
	if hdr[0] != 0 {
		return 0, false, fmt.Errorf("get status %d", hdr[0])
	}
	if int(vlen) > len(dst) {
		return 0, false, fmt.Errorf("value length %d", vlen)
	}
	if vlen > 0 {
		if _, err = io.ReadFull(r, dst[:vlen]); err != nil {
			return 0, false, err
		}
	}
	return int(vlen), false, nil
}

func readRESPInto(r *bufio.Reader, dst []byte) (n int, miss bool, err error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return 0, false, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return 0, false, fmt.Errorf("short redis line %q", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+', ':':
		return 0, false, nil
	case '-':
		return 0, false, fmt.Errorf("redis error %s", body)
	case '$':
		bn, err := strconv.Atoi(body)
		if err != nil {
			return 0, false, err
		}
		if bn < 0 {
			return 0, true, nil
		}
		if bn > len(dst) {
			return 0, false, fmt.Errorf("bulk %d exceeds buffer", bn)
		}
		if bn > 0 {
			if _, err = io.ReadFull(r, dst[:bn]); err != nil {
				return 0, false, err
			}
		}
		var crlf [2]byte
		if _, err = io.ReadFull(r, crlf[:]); err != nil {
			return 0, false, err
		}
		return bn, false, nil
	default:
		return 0, false, fmt.Errorf("redis type %q", line[:1])
	}
}

func readGet(r *bufio.Reader) (val []byte, miss bool, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return nil, false, err
	}
	vlen := binary.LittleEndian.Uint32(hdr[1:])
	if hdr[0] == 1 {
		if vlen != 0 {
			return nil, false, fmt.Errorf("miss with length %d", vlen)
		}
		return nil, true, nil
	}
	if hdr[0] != 0 {
		return nil, false, fmt.Errorf("get status %d", hdr[0])
	}
	if vlen > 32*1024*1024 {
		return nil, false, fmt.Errorf("value length %d", vlen)
	}
	val = make([]byte, vlen)
	if vlen > 0 {
		if _, err = io.ReadFull(r, val); err != nil {
			return nil, false, err
		}
	}
	return val, false, nil
}

type statResult struct {
	gets, hits, misses, sets, dels, evicts, live uint64
}

func readStat(r *bufio.Reader) (statResult, error) {
	var raw [1 + 7*8]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return statResult{}, err
	}
	if raw[0] != 0 {
		return statResult{}, fmt.Errorf("stat status %d", raw[0])
	}
	var s statResult
	s.gets = binary.LittleEndian.Uint64(raw[1:])
	s.hits = binary.LittleEndian.Uint64(raw[9:])
	s.misses = binary.LittleEndian.Uint64(raw[17:])
	s.sets = binary.LittleEndian.Uint64(raw[25:])
	s.dels = binary.LittleEndian.Uint64(raw[33:])
	s.evicts = binary.LittleEndian.Uint64(raw[41:])
	s.live = binary.LittleEndian.Uint64(raw[49:])
	return s, nil
}

func appendRESPGet(dst, key []byte) []byte {
	return appendRESP(dst, []byte("GET"), key)
}

func appendRESPSet(dst, key, val []byte) []byte {
	return appendRESP(dst, []byte("SET"), key, val)
}

func appendRESP(dst []byte, parts ...[]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(parts)), 10)
	dst = append(dst, '\r', '\n')
	for _, p := range parts {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(p)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, p...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

func readRESP(r *bufio.Reader) (bulk []byte, miss bool, err error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, false, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, false, fmt.Errorf("short redis line %q", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+', ':':
		return []byte(body), false, nil
	case '-':
		return nil, false, fmt.Errorf("redis error %s", body)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, false, err
		}
		if n < 0 {
			return nil, true, nil
		}
		buf := make([]byte, n)
		if _, err = io.ReadFull(r, buf); err != nil {
			return nil, false, err
		}
		if _, err = io.ReadFull(r, make([]byte, 2)); err != nil {
			return nil, false, err
		}
		return buf, false, nil
	default:
		return nil, false, fmt.Errorf("redis type %q", line[:1])
	}
}

func formatInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
