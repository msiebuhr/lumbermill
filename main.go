package main

import (
	"crypto/tls"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	auth "github.com/heroku/authenticater"
)

type shutdownChan chan struct{}

type clientFunc func() *http.Client

const (
	defaultClientTimeout = 20 * time.Second
	pointChannelCapacity = 500000
	postersPerHost       = 6
)

var (
	connectionCloser = make(chan struct{})
	debug            = os.Getenv("DEBUG") == "true"
)

func (s shutdownChan) Close() error {
	s <- struct{}{}
	return nil
}

func awaitSignals(ss ...io.Closer) {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	log.Printf("Got signal: %q", sig)
	for _, s := range ss {
		s.Close()
	}
}

func awaitShutdown(shutdownChan shutdownChan, server *server) {
	<-shutdownChan
	log.Printf("waiting for inflight requests to finish.")
	server.Wait()
	log.Printf("Shutdown complete.")
}

func newClientFunc() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: os.Getenv("INFLUXDB_SKIP_VERIFY") == "true"},
		},
		Timeout: defaultClientTimeout,
	}
}

func main() {
	basicAuther, err := auth.NewBasicAuthFromString(os.Getenv("CRED_STORE"))
	if err != nil {
		log.Fatalf("Unable to parse credentials from CRED_STORE=%q: err=%q", os.Getenv("CRED_STORE"), err)
	}

	shutdownChan := make(shutdownChan)
	server := newServer(&http.Server{Addr: ":" + os.Getenv("PORT")}, basicAuther)

	log.Printf("Starting up")
	go server.Run(5 * time.Minute)

	var closers []io.Closer
	closers = append(closers, server)
	closers = append(closers, shutdownChan)

	go awaitSignals(closers...)
	awaitShutdown(shutdownChan, server)
}
