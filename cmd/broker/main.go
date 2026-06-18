package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pert/message-broker/pkg/broker"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data", "./data/broker", "data directory")
	brokerID := flag.String("id", "broker-1", "broker id")
	managerURL := flag.String("manager", "http://localhost:8081", "queue manager url")
	flag.Parse()

	storage, err := broker.NewStorage(*dataDir)
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	srv := broker.NewServer(storage)
	httpServer := &http.Server{Addr: *addr, Handler: srv.Handler()}

	go registerWithManager(*managerURL, *brokerID, "http://"+trimPortHost(*addr))

	go func() {
		log.Printf("broker listening on %s, data=%s", *addr, *dataDir)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("broker shutting down")
}

func registerWithManager(managerURL, id, url string) {
	// Replace listen addr with reachable URL for docker
	if envURL := os.Getenv("BROKER_PUBLIC_URL"); envURL != "" {
		url = envURL
	}
	body, _ := json.Marshal(map[string]string{"id": id, "url": url})
	for {
		resp, err := http.Post(managerURL+"/brokers/register", "application/json", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Printf("registered with manager as %s", url)
				return
			}
		}
		log.Printf("waiting to register with manager...")
		registerLoop(managerURL, id, url)
		return
	}
}

func registerLoop(managerURL, id, url string) {
	body, _ := json.Marshal(map[string]string{"id": id, "url": url})
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		resp, err := http.Post(managerURL+"/brokers/register", "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			log.Printf("registered with manager as %s", url)
			return
		}
	}
}

func trimPortHost(addr string) string {
	if addr[0] == ':' {
		return "localhost" + addr
	}
	return addr
}
