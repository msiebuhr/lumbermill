package main

import (
	"log"
	"net/http"
	"sync"
	"time"

	auth "github.com/heroku/authenticater"

	"github.com/prometheus/client_golang/prometheus"
)

type server struct {
	sync.WaitGroup
	connectionCloser chan struct{}
	hashRing         *hashRing
	http             *http.Server
	shutdownChan     shutdownChan
	isShuttingDown   bool
	credStore        map[string]string
}

func newServer(httpServer *http.Server, ath auth.Authenticater, hashRing *hashRing) *server {
	s := &server{
		connectionCloser: make(chan struct{}),
		shutdownChan:     make(chan struct{}),
		http:             httpServer,
		hashRing:         hashRing,
		credStore:        make(map[string]string),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/drain", auth.WrapAuth(ath,
		func(w http.ResponseWriter, r *http.Request) {
			s.serveDrain(w, r)
			s.recycleConnection(w)
		}))

	mux.HandleFunc("/health", s.serveHealth)
	mux.Handle("/metrics", prometheus.UninstrumentedHandler())

	s.http.Handler = mux

	return s
}

func (s *server) Close() error {
	s.shutdownChan <- struct{}{}
	return nil
}

func (s *server) scheduleConnectionRecycling(after time.Duration) {
	for !s.isShuttingDown {
		time.Sleep(after)
		s.connectionCloser <- struct{}{}
	}
}

func (s *server) recycleConnection(w http.ResponseWriter) {
	select {
	case <-s.connectionCloser:
		w.Header().Set("Connection", "close")
	default:
		if s.isShuttingDown {
			w.Header().Set("Connection", "close")
		}
	}
}

func (s *server) Run(connRecycle time.Duration) {
	go s.awaitShutdown()
	go s.scheduleConnectionRecycling(connRecycle)

	if err := s.http.ListenAndServe(); err != nil {
		log.Fatalln("Unable to start HTTP server: ", err)
	}
}

// Serves a 200 OK, unless shutdown has been requested.
// Shutting down serves a 503 since that's how ELBs implement connection draining.
func (s *server) serveHealth(w http.ResponseWriter, r *http.Request) {
	if s.isShuttingDown {
		http.Error(w, "Shutting Down", 503)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *server) awaitShutdown() {
	<-s.shutdownChan
	log.Printf("Shutting down.")
	s.isShuttingDown = true
}
