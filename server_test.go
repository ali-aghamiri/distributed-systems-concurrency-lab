package kvserver

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func connect(t *testing.T, s *Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	return c, bufio.NewReader(c)
}

func readLine(t *testing.T, r *bufio.Reader, want string) {
	t.Helper()
	got, err := r.ReadString('\n')
	if err != nil || got != want {
		t.Fatalf("got %q (%v), want %q", got, err, want)
	}
}

func TestCommandsAndSharedStore(t *testing.T) {
	s, err := Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, _ := connect(t, s)
	defer a.Close()
	b, reader := connect(t, s)
	defer b.Close()
	fmt.Fprint(a, "Put:item:one\nPut:item:two\n")
	// The client's own subsequent Get is a barrier for its two Put events.
	fmt.Fprint(a, "Get:item\n")
	ra := bufio.NewReader(a)
	readLine(t, ra, "item:one\n")
	readLine(t, ra, "item:two\n")
	fmt.Fprint(b, "Update:item:one:three\nGet:item\n")
	readLine(t, reader, "item:three\n")
	readLine(t, reader, "item:two\n")
	fmt.Fprint(b, "Delete:item\nGet:item\nBogus:item\n")
	readLine(t, reader, "ERROR:unknown command\n")
}

func TestConcurrentClientsAndClose(t *testing.T) {
	s, err := Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, r := connect(t, s)
			defer c.Close()
			fmt.Fprintf(c, "Put:key%d:value%d\nGet:key%d\n", i, i, i)
			readLine(t, r, fmt.Sprintf("key%d:value%d\n", i, i))
		}(i)
	}
	wg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for {
		stats, err := s.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.Active == 0 && stats.Dropped == 30 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connections not accounted for: %+v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stats(); err != ErrClosed {
		t.Fatalf("Stats after Close: %v", err)
	}
}
