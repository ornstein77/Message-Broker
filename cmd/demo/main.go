package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pert/message-broker/pkg/sdk"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "data-safety":
		runDataSafety(os.Args[2:])
	case "load-balance":
		runLoadBalance(os.Args[2:])
	case "restart-offset":
		runRestartOffset(os.Args[2:])
	case "multiple-groups":
		runMultipleGroups(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: demo <case> [flags]

Cases:
  data-safety       Publish 1000 msgs, verify all readable after broker restart
  load-balance      Two subscribers in one group split 10 messages
  restart-offset    Subscriber resumes from saved offset after pause
  multiple-groups   Same messages delivered to two independent groups

Common flags:
  -broker URL       (default http://localhost:8080)
  -manager URL      (default http://localhost:8081)
  -topic NAME       (default demo)
`)
}

type commonFlags struct {
	broker  string
	manager string
	topic   string
}

func parseCommon(args []string) commonFlags {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	c := commonFlags{}
	fs.StringVar(&c.broker, "broker", env("BROKER_URL", "http://localhost:8080"), "")
	fs.StringVar(&c.manager, "manager", env("MANAGER_URL", "http://localhost:8081"), "")
	fs.StringVar(&c.topic, "topic", "demo-case", "")
	fs.Parse(args)
	return c
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func runDataSafety(args []string) {
	c := parseCommon(args)
	client := sdk.NewClient(c.broker, c.manager)
	pub := sdk.NewPublisher(client)
	group := "data-safety-group"

	log.Println("=== Data Safety: publishing 1000 messages ===")
	for i := 1; i <= 1000; i++ {
		if _, err := pub.Publish(c.topic, fmt.Sprintf("payload-%d", i)); err != nil {
			log.Fatalf("publish: %v", err)
		}
	}
	log.Println("Published 1000 messages. Now kill broker (kill -9) and restart it, then press Enter...")
	fmt.Scanln()

	sub := sdk.NewSubscriber(client, c.topic, group, "data-safety-consumer")
	var count int
	for {
		msgs, err := sub.Poll(50)
		if err != nil {
			log.Fatalf("poll: %v", err)
		}
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			if err := sub.Ack(m.Offset); err != nil {
				log.Fatalf("ack: %v", err)
			}
			count++
		}
	}
	if count != 1000 {
		log.Fatalf("FAIL: expected 1000 messages, got %d", count)
	}
	log.Println("PASS: all 1000 messages read after broker restart")
}

func runLoadBalance(args []string) {
	c := parseCommon(args)
	c.topic = "load-balance"
	client := sdk.NewClient(c.broker, c.manager)
	pub := sdk.NewPublisher(client)
	group := "lb-group"

	log.Println("=== Load Balancing: 10 messages, 2 subscribers ===")
	for i := 1; i <= 10; i++ {
		if _, err := pub.Publish(c.topic, fmt.Sprintf("msg-%d", i)); err != nil {
			log.Fatalf("publish: %v", err)
		}
	}

	type result struct {
		id    string
		count int
	}
	ch := make(chan result, 2)

	for _, id := range []string{"sub-a", "sub-b"} {
		go func(consumerID string) {
			sub := sdk.NewSubscriber(client, c.topic, group, consumerID)
			n := 0
			for {
				msgs, err := sub.Poll(1)
				if err != nil {
					log.Printf("%s error: %v", consumerID, err)
					break
				}
				if len(msgs) == 0 {
					time.Sleep(200 * time.Millisecond)
					msgs, _ = sub.Poll(1)
					if len(msgs) == 0 {
						break
					}
				}
				for _, m := range msgs {
					sub.Ack(m.Offset)
					n++
				}
			}
			ch <- result{id: consumerID, count: n}
		}(id)
	}

	r1 := <-ch
	r2 := <-ch
	log.Printf("%s processed %d, %s processed %d", r1.id, r1.count, r2.id, r2.count)
	if r1.count+r2.count != 10 {
		log.Fatalf("FAIL: total processed %d != 10", r1.count+r2.count)
	}
	if r1.count == 0 || r2.count == 0 {
		log.Fatalf("FAIL: load not balanced (one subscriber got 0)")
	}
	log.Println("PASS: messages distributed across subscribers in the same group")
}

func runRestartOffset(args []string) {
	c := parseCommon(args)
	c.topic = "restart-offset"
	client := sdk.NewClient(c.broker, c.manager)
	pub := sdk.NewPublisher(client)
	group := "restart-group"
	consumerID := "restart-consumer"

	log.Println("=== Restart from Offset ===")
	sub := sdk.NewSubscriber(client, c.topic, group, consumerID)

	for i := 1; i <= 5; i++ {
		pub.Publish(c.topic, fmt.Sprintf("before-%d", i))
	}
	for i := 0; i < 5; i++ {
		msgs, _ := sub.Poll(1)
		for _, m := range msgs {
			sub.Ack(m.Offset)
		}
	}
	off, _ := sub.CommittedOffset()
	log.Printf("Committed offset after 5 messages: %d", off)

	log.Println("Simulating subscriber stop — publishing 5 new messages...")
	for i := 1; i <= 5; i++ {
		pub.Publish(c.topic, fmt.Sprintf("after-%d", i))
	}

	sub2 := sdk.NewSubscriber(client, c.topic, group, consumerID)
	var payloads []string
	for {
		msgs, err := sub2.Poll(10)
		if err != nil {
			log.Fatalf("poll: %v", err)
		}
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			payloads = append(payloads, m.Payload)
			sub2.Ack(m.Offset)
		}
	}
	if len(payloads) != 5 {
		log.Fatalf("FAIL: expected 5 new messages, got %d: %v", len(payloads), payloads)
	}
	if payloads[0] != "after-1" {
		log.Fatalf("FAIL: expected to resume at after-1, got %q", payloads[0])
	}
	log.Println("PASS: subscriber resumed from saved offset")
}

func runMultipleGroups(args []string) {
	c := parseCommon(args)
	c.topic = "multi-group"
	client := sdk.NewClient(c.broker, c.manager)
	pub := sdk.NewPublisher(client)

	log.Println("=== Multiple Groups: same topic, two groups ===")
	for i := 1; i <= 5; i++ {
		pub.Publish(c.topic, fmt.Sprintf("shared-%d", i))
	}

	readGroup := func(group string) []string {
		sub := sdk.NewSubscriber(client, c.topic, group, group+"-consumer")
		var out []string
		for {
			msgs, err := sub.Poll(10)
			if err != nil {
				log.Fatalf("poll: %v", err)
			}
			if len(msgs) == 0 {
				break
			}
			for _, m := range msgs {
				out = append(out, m.Payload)
				sub.Ack(m.Offset)
			}
		}
		return out
	}

	a := readGroup("group-a")
	b := readGroup("group-b")
	if len(a) != 5 || len(b) != 5 {
		log.Fatalf("FAIL: group-a=%d group-b=%d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			log.Fatalf("FAIL: groups received different data: %v vs %v", a, b)
		}
	}
	log.Println("PASS: both groups received all 5 messages independently")
}
