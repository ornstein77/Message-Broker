package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pert/message-broker/pkg/sdk"
)

func main() {
	broker := flag.String("broker", "http://localhost:8080", "broker url")
	manager := flag.String("manager", "http://localhost:8081", "manager url")
	topic := flag.String("topic", "demo", "topic name")
	group := flag.String("group", "group-1", "consumer group id")
	consumerID := flag.String("id", "", "consumer instance id")
	limit := flag.Int64("limit", 1, "messages per poll")
	interval := flag.Duration("interval", 500*time.Millisecond, "poll interval")
	maxMessages := flag.Int("max", 0, "stop after N messages (0 = unlimited)")
	duration := flag.Duration("duration", 0, "run for duration (0 = until signal)")
	flag.Parse()

	client := sdk.NewClient(*broker, *manager)
	sub := sdk.NewSubscriber(client, *topic, *group, *consumerID)

	ctx, cancel := context.WithCancel(context.Background())
	if *duration > 0 {
		tctx, tcancel := context.WithTimeout(ctx, *duration)
		defer tcancel()
		ctx = tctx
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sig:
			cancel()
		case <-ctx.Done():
		}
	}()

	var processed int64
	log.Printf("subscriber %s group=%s topic=%s started", sub.ConsumerID(), *group, *topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("subscriber %s stopped, processed=%d", sub.ConsumerID(), processed)
			return
		default:
		}

		msgs, err := sub.Poll(*limit)
		if err != nil {
			log.Fatalf("poll: %v", err)
		}
		for _, msg := range msgs {
			fmt.Printf("[%s] offset=%d payload=%s\n", sub.ConsumerID(), msg.Offset, msg.Payload)
			if err := sub.Ack(msg.Offset); err != nil {
				log.Fatalf("ack: %v", err)
			}
			n := atomic.AddInt64(&processed, 1)
			if *maxMessages > 0 && int(n) >= *maxMessages {
				log.Printf("subscriber %s reached max=%d", sub.ConsumerID(), *maxMessages)
				return
			}
		}
		time.Sleep(*interval)
	}
}
