package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pert/message-broker/pkg/models"
)

type Storage struct {
	dir  string
	mu   sync.RWMutex
	meta map[string]*topicMeta
}

type topicMeta struct {
	file   *os.File
	count  int64
	writer *bufio.Writer
}

func NewStorage(dir string) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Storage{dir: dir, meta: make(map[string]*topicMeta)}
	if err := s.loadExisting(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Storage) topicPath(topic string) string {
	return filepath.Join(s.dir, topic+".log")
}

func (s *Storage) loadExisting() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".log" {
			continue
		}
		topic := e.Name()[:len(e.Name())-4]
		if err := s.openTopic(topic); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) openTopic(topic string) error {
	path := s.topicPath(topic)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	count, err := countLines(path)
	if err != nil {
		f.Close()
		return err
	}
	s.meta[topic] = &topicMeta{
		file:   f,
		count:  count,
		writer: bufio.NewWriter(f),
	}
	return nil
}

func countLines(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	var count int64
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			count++
		}
	}
	return count, scanner.Err()
}

func (s *Storage) ensureTopic(topic string) error {
	if _, ok := s.meta[topic]; ok {
		return nil
	}
	return s.openTopic(topic)
}

func (s *Storage) Append(topic, payload string) (models.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureTopic(topic); err != nil {
		return models.Message{}, err
	}
	meta := s.meta[topic]
	msg := models.Message{
		Offset:    meta.count,
		Topic:     topic,
		Payload:   payload,
		Timestamp: models.Now(),
	}
	line, err := json.Marshal(msg)
	if err != nil {
		return models.Message{}, err
	}
	if _, err := meta.writer.Write(append(line, '\n')); err != nil {
		return models.Message{}, err
	}
	if err := meta.writer.Flush(); err != nil {
		return models.Message{}, err
	}
	if err := meta.file.Sync(); err != nil {
		return models.Message{}, err
	}
	meta.count++
	return msg, nil
}

func (s *Storage) Read(topic string, offset, limit int64) ([]models.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.topicPath(topic)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var result []models.Message
	var current int64
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		if current >= offset {
			if limit > 0 && int64(len(result)) >= limit {
				break
			}
			var msg models.Message
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				return nil, fmt.Errorf("corrupt log at offset %d: %w", current, err)
			}
			result = append(result, msg)
		}
		current++
	}
	return result, scanner.Err()
}

func (s *Storage) Count(topic string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if meta, ok := s.meta[topic]; ok {
		return meta.count
	}
	count, _ := countLines(s.topicPath(topic))
	return count
}

func (s *Storage) Topics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.meta))
	for name := range s.meta {
		names = append(names, name)
	}
	return names
}
