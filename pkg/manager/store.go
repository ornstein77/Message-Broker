package manager

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pert/message-broker/pkg/models"
)

type Store struct {
	path string
	mu   sync.RWMutex
	data *Registry
}

type Registry struct {
	Brokers map[string]*models.BrokerInfo `json:"brokers"`
	Topics  map[string]*TopicState        `json:"topics"`
}

type TopicState struct {
	Groups map[string]*GroupState `json:"groups"`
}

type GroupState struct {
	CommittedOffset int64                `json:"committed_offset"`
	MaxAssigned     int64                `json:"max_assigned"`
	Pending         map[int64]string     `json:"pending"`
	Consumers       map[string]time.Time `json:"consumers"`
}

func NewStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		path: path,
		data: &Registry{
			Brokers: make(map[string]*models.BrokerInfo),
			Topics:  make(map[string]*TopicState),
		},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return s.save()
		}
		return err
	}
	if len(b) == 0 {
		return nil
	}
	var reg Registry
	if err := json.Unmarshal(b, &reg); err != nil {
		return err
	}
	if reg.Brokers == nil {
		reg.Brokers = make(map[string]*models.BrokerInfo)
	}
	if reg.Topics == nil {
		reg.Topics = make(map[string]*TopicState)
	}
	for _, topic := range reg.Topics {
		if topic.Groups == nil {
			topic.Groups = make(map[string]*GroupState)
		}
		for _, g := range topic.Groups {
			if g.Pending == nil {
				g.Pending = make(map[int64]string)
			}
			if g.Consumers == nil {
				g.Consumers = make(map[string]time.Time)
			}
		}
	}
	s.data = &reg
	return nil
}

func (s *Store) save() error {
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	var lastErr error
	for i := 0; i < 5; i++ {
		if err := os.Rename(tmp, s.path); err != nil {
			lastErr = err
			time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
			continue
		}
		return nil
	}
	if err := os.WriteFile(s.path, b, 0o644); err != nil {
		return err
	}
	_ = os.Remove(tmp)
	return lastErr
}

func (s *Store) ensureTopicGroup(topic, group string) *GroupState {
	if _, ok := s.data.Topics[topic]; !ok {
		s.data.Topics[topic] = &TopicState{Groups: make(map[string]*GroupState)}
	}
	t := s.data.Topics[topic]
	if _, ok := t.Groups[group]; !ok {
		t.Groups[group] = &GroupState{
			CommittedOffset: -1,
			MaxAssigned:     -1,
			Pending:         make(map[int64]string),
			Consumers:       make(map[string]time.Time),
		}
	}
	return t.Groups[group]
}

func (g *GroupState) advanceCommitted() {
	for {
		next := g.CommittedOffset + 1
		if next > g.MaxAssigned {
			return
		}
		if _, pending := g.Pending[next]; pending {
			return
		}
		g.CommittedOffset = next
	}
}

func (s *Store) RegisterBroker(id, url string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := models.Now()
	s.data.Brokers[id] = &models.BrokerInfo{
		ID:       id,
		URL:      url,
		LastSeen: now,
		Active:   true,
	}
	return s.save()
}

func (s *Store) MarkBrokersStale(ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := models.Now()
	changed := false
	for _, b := range s.data.Brokers {
		active := now.Sub(b.LastSeen) <= ttl
		if b.Active != active {
			b.Active = active
			changed = true
		}
	}
	if changed {
		return s.save()
	}
	return nil
}

func (s *Store) GetActiveBrokerURL() (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, b := range s.data.Brokers {
		if b.Active && b.URL != "" {
			return b.URL, true
		}
	}
	for _, b := range s.data.Brokers {
		if b.URL != "" {
			return b.URL, true
		}
	}
	return "", false
}

func (s *Store) TouchConsumer(topic, group, consumerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	g := s.ensureTopicGroup(topic, group)
	g.Consumers[consumerID] = models.Now()
	return s.save()
}

func (s *Store) AssignNext(topic, group, consumerID string, messageCount int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g := s.ensureTopicGroup(topic, group)
	g.Consumers[consumerID] = models.Now()

	for offset := int64(0); offset < messageCount; offset++ {
		if offset <= g.CommittedOffset {
			continue
		}
		if _, pending := g.Pending[offset]; pending {
			continue
		}
		g.Pending[offset] = consumerID
		if offset > g.MaxAssigned {
			g.MaxAssigned = offset
		}
		if err := s.save(); err != nil {
			return -1, false, err
		}
		return offset, true, nil
	}
	return -1, false, nil
}

func (s *Store) AssignBatch(topic, group, consumerID string, messageCount, limit int64) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g := s.ensureTopicGroup(topic, group)
	g.Consumers[consumerID] = models.Now()

	var offsets []int64
	for offset := int64(0); offset < messageCount && int64(len(offsets)) < limit; offset++ {
		if offset <= g.CommittedOffset {
			continue
		}
		if _, pending := g.Pending[offset]; pending {
			continue
		}
		g.Pending[offset] = consumerID
		if offset > g.MaxAssigned {
			g.MaxAssigned = offset
		}
		offsets = append(offsets, offset)
	}
	if len(offsets) == 0 {
		return nil, nil
	}
	if err := s.save(); err != nil {
		return nil, err
	}
	return offsets, nil
}

func (s *Store) Ack(topic, group, consumerID string, offsets []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	g := s.ensureTopicGroup(topic, group)
	for _, offset := range offsets {
		owner, ok := g.Pending[offset]
		if !ok || owner != consumerID {
			return fmt.Errorf("offset %d not assigned to consumer %s", offset, consumerID)
		}
		delete(g.Pending, offset)
	}
	g.advanceCommitted()
	return s.save()
}

func (s *Store) Status(topicCounts map[string]int64) models.StatusResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := models.StatusResponse{
		Brokers: make([]models.BrokerInfo, 0, len(s.data.Brokers)),
		Topics:  make([]models.TopicStatus, 0),
	}
	for _, b := range s.data.Brokers {
		resp.Brokers = append(resp.Brokers, *b)
	}

	seen := make(map[string]bool)
	for topic, state := range s.data.Topics {
		seen[topic] = true
		resp.Topics = append(resp.Topics, s.topicStatus(topic, state, topicCounts[topic]))
	}
	for topic, count := range topicCounts {
		if !seen[topic] {
			resp.Topics = append(resp.Topics, models.TopicStatus{
				Topic:        topic,
				MessageCount: count,
			})
		}
	}
	return resp
}

func (s *Store) topicStatus(topic string, state *TopicState, count int64) models.TopicStatus {
	ts := models.TopicStatus{
		Topic:        topic,
		MessageCount: count,
		Groups:       make([]models.GroupStatus, 0, len(state.Groups)),
	}
	for groupID, g := range state.Groups {
		ts.Groups = append(ts.Groups, models.GroupStatus{
			GroupID:         groupID,
			CommittedOffset: g.CommittedOffset,
			PendingCount:    len(g.Pending),
			ActiveConsumers: len(g.Consumers),
		})
	}
	return ts
}

func (s *Store) GetCommittedOffset(topic, group string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if t, ok := s.data.Topics[topic]; ok {
		if g, ok := t.Groups[group]; ok {
			return g.CommittedOffset
		}
	}
	return -1
}
