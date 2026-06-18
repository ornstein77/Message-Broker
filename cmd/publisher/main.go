package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/pert/message-broker/pkg/sdk"
)

func main() {
	broker := flag.String("broker", "http://localhost:8080", "broker url")
	manager := flag.String("manager", "http://localhost:8081", "manager url")
	topic := flag.String("topic", "demo", "topic name")
	message := flag.String("message", "", "single message payload")
	count := flag.Int("count", 0, "number of messages to send")
	prefix := flag.String("prefix", "msg", "message prefix when using -count")
	flag.Parse()

	client := sdk.NewClient(*broker, *manager)
	pub := sdk.NewPublisher(client)

	if *count > 0 {
		for i := 1; i <= *count; i++ {
			payload := fmt.Sprintf("%s-%d", *prefix, i)
			msg, err := pub.Publish(*topic, payload)
			if err != nil {
				log.Fatalf("publish %d: %v", i, err)
			}
			if i%100 == 0 || i == *count {
				log.Printf("published %d/%d (offset=%d)", i, *count, msg.Offset)
			}
		}
		log.Printf("done: published %d messages to topic %q", *count, *topic)
		return
	}

	if *message == "" {
		log.Fatal("provide -message or -count")
	}
	msg, err := pub.Publish(*topic, *message)
	if err != nil {
		log.Fatalf("publish: %v", err)
	}
	log.Printf("published offset=%d payload=%q", msg.Offset, msg.Payload)
}
