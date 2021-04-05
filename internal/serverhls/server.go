package serverhls

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aler9/rtsp-simple-server/internal/logger"
)

func newEmptyTimer() *time.Timer {
	t := time.NewTimer(0)
	<-t.C
	return t
}

// Parent is implemented by program.
type Parent interface {
	Log(logger.Level, string, ...interface{})
}

type Server struct {
	ln net.Listener
	s  *http.Server

	// in
	terminate chan struct{}

	// out
	request chan string
	done    chan struct{}
}

func New(
	listenIP string,
	port int,
	parent Parent,
) (*Server, error) {
	address := listenIP + ":" + strconv.FormatInt(int64(port), 10)
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	s := &Server{
		ln:        ln,
		terminate: make(chan struct{}),
		request:   make(chan string),
		done:      make(chan struct{}),
	}

	s.s = &http.Server{
		Handler: s,
	}

	go s.run()

	parent.Log(logger.Info, "[HLS listener] opened on "+address)

	return s, nil
}

func (s *Server) Close() {
	close(s.terminate)
	<-s.done
}

func (s *Server) run() {
	defer close(s.done)

	go s.s.Serve(s.ln)

outer:
	for {
		select {
		case <-s.terminate:
			break outer
		}
	}

	s.s.Shutdown(context.Background())
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// remove leading prefix
	path := r.URL.Path[1:]

	// remove trailing prefix
	if !strings.HasSuffix(path, "/") {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	path = path[:len(path)-1]

	if path == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	s.request <- path

	w.Write([]byte("OK\n"))
}

// Request returns a channel to handle incoming HTTP requests
func (s *Server) Request() chan string {
	return s.request
}
