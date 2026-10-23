package kvserver

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"sync/atomic"
)

var ErrClosed = errors.New("server is closed")

type Stats struct {
	Active  int
	Dropped int
}

type client struct {
	conn net.Conn
	out  chan string
}

type event struct {
	kind   byte
	client *client
	line   string
	reply  chan Stats
}

// Server keeps all mutable application state in one goroutine. Connection
// readers and writers communicate with it through channels.
type Server struct {
	listener net.Listener
	events   chan event
	done     chan struct{}
	closed   atomic.Bool
}

func Start(address string) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &Server{listener: listener, events: make(chan event, 1024), done: make(chan struct{})}
	go s.run()
	go s.accept()
	return s, nil
}

func (s *Server) Addr() net.Addr { return s.listener.Addr() }

func (s *Server) send(e event) {
	select {
	case s.events <- e:
	case <-s.done:
	}
}

func (s *Server) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		if s.closed.Load() {
			conn.Close()
			return
		}
		c := &client{conn: conn, out: make(chan string, 128)}
		s.send(event{kind: 'j', client: c})
		if s.closed.Load() {
			conn.Close()
			return
		}
		go s.read(c)
	}
}

func (s *Server) read(c *client) {
	defer s.send(event{kind: 'd', client: c})
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		s.send(event{kind: 'c', client: c, line: scanner.Text()})
	}
}

func (s *Server) write(c *client) {
	for message := range c.out {
		if _, err := c.conn.Write([]byte(message)); err != nil {
			c.conn.Close()
			return
		}
	}
}

func (s *Server) run() {
	defer close(s.done)
	clients := make(map[*client]bool)
	store := make(map[string][]string)
	dropped := 0
	drop := func(c *client) {
		if clients[c] {
			delete(clients, c)
			close(c.out)
			c.conn.Close()
			dropped++
		}
	}
	send := func(c *client, message string) {
		if !clients[c] {
			return
		}
		select {
		case c.out <- message:
		default:
			drop(c) // A slow reader must not stall other clients.
		}
	}
	for e := range s.events {
		switch e.kind {
		case 'j':
			clients[e.client] = true
			go s.write(e.client)
		case 'd':
			drop(e.client)
		case 's':
			e.reply <- Stats{Active: len(clients), Dropped: dropped}
		case 'x':
			for c := range clients {
				drop(c)
			}
			return
		case 'c':
			c := e.client
			if !clients[c] {
				continue
			}
			parts := strings.Split(e.line, ":")
			if len(parts) < 2 || parts[1] == "" {
				send(c, "ERROR:invalid request\n")
				continue
			}
			key := parts[1]
			switch parts[0] {
			case "Put":
				if len(parts) != 3 {
					send(c, "ERROR:invalid Put\n")
					continue
				}
				store[key] = append(store[key], parts[2])
			case "Get":
				if len(parts) != 2 {
					send(c, "ERROR:invalid Get\n")
					continue
				}
				for _, value := range store[key] {
					send(c, key+":"+value+"\n")
				}
			case "Delete":
				if len(parts) != 2 {
					send(c, "ERROR:invalid Delete\n")
					continue
				}
				delete(store, key)
			case "Update":
				if len(parts) != 4 {
					send(c, "ERROR:invalid Update\n")
					continue
				}
				for i, value := range store[key] {
					if value == parts[2] {
						store[key][i] = parts[3]
						break
					}
				}
			default:
				send(c, "ERROR:unknown command\n")
			}
		}
	}
}

func (s *Server) Stats() (Stats, error) {
	if s.closed.Load() {
		return Stats{}, ErrClosed
	}
	reply := make(chan Stats, 1)
	s.send(event{kind: 's', reply: reply})
	select {
	case stats := <-reply:
		return stats, nil
	case <-s.done:
		return Stats{}, ErrClosed
	}
}

func (s *Server) Close() error {
	if !s.closed.Swap(true) {
		s.listener.Close()
		s.send(event{kind: 'x'})
	}
	<-s.done
	return nil
}
