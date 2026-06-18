package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pert/message-broker/pkg/manager"
)

func main() {
	addr := flag.String("addr", ":8081", "listen address")
	registry := flag.String("registry", "./data/manager/registry.json", "registry file")
	brokerURL := flag.String("broker", "http://localhost:8080", "default broker url")
	flag.Parse()

	store, err := manager.NewStore(*registry)
	if err != nil {
		log.Fatalf("store: %v", err)
	}

	stop := make(chan struct{})
	srv := manager.NewServer(manager.Config{
		Store:         store,
		DefaultBroker: *brokerURL,
		BrokerTTL:     30 * time.Second,
	})
	srv.StartBackgroundTasks(stop)

	httpServer := &http.Server{Addr: *addr, Handler: srv.Handler()}
	go func() {
		log.Printf("manager listening on %s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)
	log.Println("manager shutting down")
}
