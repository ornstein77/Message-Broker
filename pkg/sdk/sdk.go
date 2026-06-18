package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pert/message-broker/pkg/models"
)

type Client struct {
	BrokerURL  string
	ManagerURL string
	HTTP       *http.Client
}

func NewClient(brokerURL, managerURL string) *Client {
	return &Client{
		BrokerURL:  brokerURL,
		ManagerURL: managerURL,
		HTTP:       &http.Client{Timeout: 30 * time.Second},
	}
}

type Publisher struct {
	client *Client
}

func NewPublisher(client *Client) *Publisher {
	return &Publisher{client: client}
}

func (p *Publisher) Publish(topic, payload string) (models.Message, error) {
	url := fmt.Sprintf("%s/topics/%s/messages", p.client.BrokerURL, topic)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(payload)))
	if err != nil {
		return models.Message{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.HTTP.Do(req)
	if err != nil {
		return models.Message{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return models.Message{}, fmt.Errorf("publish failed %d: %s", resp.StatusCode, string(body))
	}
	var msg models.Message
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		return models.Message{}, err
	}
	return msg, nil
}

type Subscriber struct {
	client     *Client
	topic      string
	group      string
	consumerID string
}

func NewSubscriber(client *Client, topic, group, consumerID string) *Subscriber {
	if consumerID == "" {
		consumerID = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	}
	return &Subscriber{
		client:     client,
		topic:      topic,
		group:      group,
		consumerID: consumerID,
	}
}

func (s *Subscriber) ConsumerID() string {
	return s.consumerID
}

func (s *Subscriber) Poll(limit int64) ([]models.ConsumerMessage, error) {
	url := fmt.Sprintf("%s/topics/%s/groups/%s/poll", s.client.ManagerURL, s.topic, s.group)
	body, err := json.Marshal(map[string]any{
		"consumer_id": s.consumerID,
		"limit":       limit,
	})
	if err != nil {
		return nil, err
	}
	resp, err := s.client.HTTP.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poll failed %d: %s", resp.StatusCode, string(b))
	}
	var out models.PollResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func (s *Subscriber) Ack(offsets ...int64) error {
	url := fmt.Sprintf("%s/topics/%s/groups/%s/ack", s.client.ManagerURL, s.topic, s.group)
	body, err := json.Marshal(models.AckRequest{
		ConsumerID: s.consumerID,
		Offsets:    offsets,
	})
	if err != nil {
		return err
	}
	resp, err := s.client.HTTP.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ack failed %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (s *Subscriber) CommittedOffset() (int64, error) {
	url := fmt.Sprintf("%s/topics/%s/groups/%s/offset", s.client.ManagerURL, s.topic, s.group)
	resp, err := s.client.HTTP.Get(url)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	var out struct {
		CommittedOffset int64 `json:"committed_offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return -1, err
	}
	return out.CommittedOffset, nil
}

func (s *Subscriber) PollLoop(interval time.Duration, limit int64, handler func(models.ConsumerMessage) error) error {
	if interval <= 0 {
		interval = time.Second
	}
	for {
		msgs, err := s.Poll(limit)
		if err != nil {
			return err
		}
		for _, msg := range msgs {
			if err := handler(msg); err != nil {
				return err
			}
			if err := s.Ack(msg.Offset); err != nil {
				return err
			}
		}
		time.Sleep(interval)
	}
}

func (c *Client) Status() (models.StatusResponse, error) {
	resp, err := c.HTTP.Get(c.ManagerURL + "/status")
	if err != nil {
		return models.StatusResponse{}, err
	}
	defer resp.Body.Close()
	var st models.StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return models.StatusResponse{}, err
	}
	return st, nil
}
