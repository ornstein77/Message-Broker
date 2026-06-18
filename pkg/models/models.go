package models

import "time"

type Message struct {
	Offset    int64     `json:"offset"`
	Topic     string    `json:"topic"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

type PublishRequest struct {
	Payload string `json:"payload"`
}

type PollResponse struct {
	Messages []ConsumerMessage `json:"messages"`
}

type ConsumerMessage struct {
	Offset     int64  `json:"offset"`
	Payload    string `json:"payload"`
	ConsumerID string `json:"consumer_id,omitempty"`
}

type AckRequest struct {
	ConsumerID string  `json:"consumer_id"`
	Offsets    []int64 `json:"offsets"`
}

type TopicInfo struct {
	Topic        string `json:"topic"`
	MessageCount int64  `json:"message_count"`
}

type BrokerInfo struct {
	ID       string    `json:"id"`
	URL      string    `json:"url"`
	LastSeen time.Time `json:"last_seen"`
	Active   bool      `json:"active"`
}

type GroupStatus struct {
	GroupID         string `json:"group_id"`
	CommittedOffset int64  `json:"committed_offset"`
	PendingCount    int    `json:"pending_count"`
	ActiveConsumers int    `json:"active_consumers"`
}

type TopicStatus struct {
	Topic        string        `json:"topic"`
	MessageCount int64         `json:"message_count"`
	Groups       []GroupStatus `json:"groups"`
}

type StatusResponse struct {
	Brokers []BrokerInfo  `json:"brokers"`
	Topics  []TopicStatus `json:"topics"`
}
