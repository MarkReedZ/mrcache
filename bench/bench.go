package main

import (
	"errors"
	"fmt"
	"io"
	//"crypto/rand"
	"net"
	"os"
	"time"
	"bytes"
  "flag"
  "encoding/binary"
  "strconv"
)

var(
    limitSeconds int
    numConnections int
    keyLength int
    length int
    port int
    hostname string
    batch int
    redis bool
    compression bool
    buffer bytes.Buffer
    key []byte
    val []byte
)

func formatInt(number int64) string {
    output := strconv.FormatInt(number, 10)
    startOffset := 3
    if number < 0 {
        startOffset++
    }
    for outputIndex := len(output); outputIndex > startOffset; {
        outputIndex -= 3
        output = output[:outputIndex] + "," + output[outputIndex:]
    }
    return output
}

// Client loops writing buffer and read the response for x seconds
func client(c chan int, tcpAddr *net.TCPAddr, tid int ) {
	  reply := make([]byte, 4096)
    transmits := 0
    conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil { println("Dial failed:", err.Error()); os.Exit(1); }

	  start := time.Now()
    reps := 0
    tbytes := 0
		for {

        reps += 1
        //fmt.Printf("Before write %d\n",tid);
        _, err = conn.Write(buffer.Bytes())
        //fmt.Printf("After write\n");
        if err != nil { fmt.Printf("Write Error: %v\n", err); break; }

        l := 0
        conn.SetReadDeadline(time.Now().Add(time.Second*5))
        bytes := 0
        for bytes < length*batch {
          l, err = conn.Read(reply)
          if err != nil { 
              fmt.Printf("DELME tid %d err %v\n",tid, err); 
              fmt.Printf("bytes %d of %d reps %d tid %d\n",bytes,length*batch, reps, tid );
          }
          if err != nil { break }
          //fmt.Printf("%v\n",reply[:16]);  
          bytes += l
          tbytes += l
          //fmt.Printf("bytes %d \n",bytes);
          //fmt.Printf("bytes %d vlen %d\n",bytes, binary.LittleEndian.Uint32(reply[1:6]))
          //os.Exit(1); 
        }
        //fmt.Printf("After read %d tbytes %d\n",tid, tbytes);
        
        //fmt.Printf("bytes %d of %d reps %d tid %d\n",bytes,length*batch, reps, tid );
        //fmt.Printf("%v\n",reply[:l])
        if err != nil && !errors.Is(err, io.EOF) { break }

        if time.Since(start).Seconds() >= float64(limitSeconds) { break }

        transmits++
   }
   conn.Close()
   c <- transmits
}

// Write data for the benchmark
func setup(tcpAddr *net.TCPAddr) {
    buffer.Reset()
    val = make([]byte, length);    //rand.Read(val);
    key = make([]byte, keyLength); //rand.Read(key);
    if redis {
        //buffer.WriteString("*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$16\r\n0123456789012345\r\n")

        buffer.WriteString("*3\r\n$3\r\nSET\r\n$")
        buffer.WriteString(strconv.Itoa(keyLength)); buffer.WriteString("\r\n")
        buffer.Write(key); buffer.WriteString("\r\n$")
        buffer.WriteString(strconv.Itoa(length)); buffer.WriteString("\r\n");
        buffer.Write(val); buffer.WriteString("\r\n")

    } else {
        //buffer.Write( []byte{ 0, 2, 4, 0, 16, 0, 0, 0, 1,1,1,1, 1,1,1,1 ,1,1,1,1 ,1,1,1,1 ,1,1,1,1 } )

        klen := make([]byte, 2); binary.LittleEndian.PutUint16(klen, uint16(keyLength))
        vlen := make([]byte, 4); binary.LittleEndian.PutUint32(vlen, uint32(length))
        if compression { 
          buffer.Write( []byte{0, 4} ); 
        } else { 
          buffer.Write( []byte{0, 2} ); 
        }
        buffer.Write( klen ); buffer.Write( vlen )
        buffer.Write( key );  buffer.Write( val )

    }
    conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil { println("Dial failed:", err.Error()); os.Exit(1); }
    _, err = conn.Write(buffer.Bytes())
    if err != nil { fmt.Printf("Write Error: %v\n", err); }
    conn.Close()
}

func main() {

  flag.IntVar(&port,           "p",  7000,     "port")
  flag.StringVar(&hostname,    "h",  "localhost",     "hostname")
  flag.IntVar(&numConnections, "c",  16,       "Number of connections")
  flag.IntVar(&limitSeconds,   "s",  2,        "Stop after n seconds")
  flag.IntVar(&keyLength,      "k",  16,       "Key length in bytes")
  flag.IntVar(&length,         "v",  256,      "Value length in bytes")
  flag.IntVar(&batch,          "b",  1,        "Batch n requests together")
  flag.BoolVar(&redis,         "redis", false, "Benchmark redis")
  flag.BoolVar(&compression,   "z",     false, "Enable compression")
  flag.Parse()

  if port == 7000 && redis {
    port = 6379
  }
	servAddr := fmt.Sprintf(`%s:%d`,hostname,port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
	if err != nil {
		println("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}

  setup(tcpAddr)


  fmt.Printf("Benchmarking for %d seconds with %d connections and a batch size of %d \n", limitSeconds, numConnections, batch);

  fmt.Printf("  Get %db\n", length);
  buffer.Reset()
  for i := 0; i < batch; i++ {
      if redis {
          buffer.WriteString("*2\r\n$3\r\nGET\r\n$")
          buffer.WriteString(strconv.Itoa(keyLength)); buffer.WriteString("\r\n")
          buffer.Write(key); buffer.WriteString("\r\n")
      } else {
          if compression { 
            buffer.Write( []byte{0, 3} )
          } else { 
            buffer.Write( []byte{0, 1} )
          }
          klen := make([]byte, 2); binary.LittleEndian.PutUint16(klen, uint16(keyLength))
          buffer.Write(klen)
          buffer.Write(key)
      }
  }

  c := make(chan int)
	start := time.Now()
	for i := 0; i < numConnections; i++ {
    go client( c, tcpAddr, i )
	}
  transmits := 0; for i := 0; i < numConnections; i++ { transmits += <-c }

	elapsed := time.Since(start)
	latency := float64(elapsed.Microseconds()) / float64(transmits)
	speed := int64((float64(transmits) / float64(elapsed.Seconds())) * float64(batch))
	fmt.Printf("    %s commands/second, mean latency %.1fu\n", formatInt(speed), latency)
  //fmt.Printf("    Took %s to perform %d requests\n", elapsed, transmits*batch)
	//fmt.Printf("    Mean latency is %.1f microsecond\n", latency)
	//fmt.Printf("    Resulting in %.1f commands/second\n", speed)

}
