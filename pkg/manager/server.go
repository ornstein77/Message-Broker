package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/pert/message-broker/pkg/models"
)

type Server struct {
	store         *Store
	brokerURL     string
	brokerTTL     time.Duration
	defaultBroker string
	mux           *http.ServeMux
	client        *http.Client
}

type Config struct {
	Store         *Store
	DefaultBroker string
	BrokerTTL     time.Duration
}

func NewServer(cfg Config) *Server {
	if cfg.BrokerTTL == 0 {
		cfg.BrokerTTL = 30 * time.Second
	}
	s := &Server{
		store:         cfg.Store,
		defaultBroker: cfg.DefaultBroker,
		brokerTTL:     cfg.BrokerTTL,
		mux:           http.NewServeMux(),
		client:        &http.Client{Timeout: 10 * time.Second},
	}
	if cfg.DefaultBroker != "" {
		s.brokerURL = cfg.DefaultBroker
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /status", s.handleStatus)
	s.mux.HandleFunc("POST /brokers/register", s.handleRegisterBroker)
	s.mux.HandleFunc("POST /topics/{topic}/groups/{group}/poll", s.handlePoll)
	s.mux.HandleFunc("POST /topics/{topic}/groups/{group}/ack", s.handleAck)
	s.mux.HandleFunc("GET /topics/{topic}/groups/{group}/offset", s.handleOffset)
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) StartBackgroundTasks(stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if err := s.store.MarkBrokersStale(s.brokerTTL); err != nil {
					log.Printf("mark stale: %v", err)
				}
				s.refreshBrokerURL()
			}
		}
	}()
}

func (s *Server) refreshBrokerURL() {
	if url, ok := s.store.GetActiveBrokerURL(); ok {
		s.brokerURL = url
	}
}

func (s *Server) brokerBaseURL() (string, error) {
	if s.brokerURL != "" {
		return s.brokerURL, nil
	}
	if s.defaultBroker != "" {
		return s.defaultBroker, nil
	}
	if url, ok := s.store.GetActiveBrokerURL(); ok {
		s.brokerURL = url
		return url, nil
	}
	return "", fmt.Errorf("no active broker registered")
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleRegisterBroker(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.URL == "" {
		http.Error(w, "id and url required", http.StatusBadRequest)
		return
	}
	if err := s.store.RegisterBroker(req.ID, req.URL); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.brokerURL = req.URL
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (s *Server) handleOffset(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	group := r.PathValue("group")
	writeJSON(w, http.StatusOK, map[string]int64{
		"committed_offset": s.store.GetCommittedOffset(topic, group),
	})
}

func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	group := r.PathValue("group")

	var req struct {
		ConsumerID string `json:"consumer_id"`
		Limit      int64  `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && r.ContentLength > 0 {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ConsumerID == "" {
		http.Error(w, "consumer_id required", http.StatusBadRequest)
		return
	}
	if req.Limit <= 0 {
		req.Limit = 1
	}
	if req.Limit > 100 {
		req.Limit = 100
	}

	count, err := s.brokerMessageCount(topic)
	if err != nil {
		log.Printf("broker count: %v", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	offsets, err := s.store.AssignBatch(topic, group, req.ConsumerID, count, req.Limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(offsets) == 0 {
		writeJSON(w, http.StatusOK, models.PollResponse{Messages: nil})
		return
	}

	msgs, err := s.brokerRead(topic, offsets[0], int64(len(offsets)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	byOffset := make(map[int64]models.Message, len(msgs))
	for _, m := range msgs {
		byOffset[m.Offset] = m
	}

	resp := models.PollResponse{Messages: make([]models.ConsumerMessage, 0, len(offsets))}
	for _, off := range offsets {
		if m, ok := byOffset[off]; ok {
			resp.Messages = append(resp.Messages, models.ConsumerMessage{
				Offset:     m.Offset,
				Payload:    m.Payload,
				ConsumerID: req.ConsumerID,
			})
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	group := r.PathValue("group")

	var req models.AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.ConsumerID == "" || len(req.Offsets) == 0 {
		http.Error(w, "consumer_id and offsets required", http.StatusBadRequest)
		return
	}
	if err := s.store.Ack(topic, group, req.ConsumerID, req.Offsets); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	counts := make(map[string]int64)
	if base, err := s.brokerBaseURL(); err == nil {
		if topics, err := s.fetchBrokerTopics(base); err == nil {
			for _, t := range topics {
				if c, err := s.brokerMessageCount(t); err == nil {
					counts[t] = c
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, s.store.Status(counts))
}

func (s *Server) fetchBrokerTopics(base string) ([]string, error) {
	resp, err := s.client.Get(base + "/topics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Topics []string `json:"topics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Topics, nil
}

func (s *Server) brokerMessageCount(topic string) (int64, error) {
	base, err := s.brokerBaseURL()
	if err != nil {
		return 0, err
	}
	resp, err := s.client.Get(base + "/topics/" + topic)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("broker status %d: %s", resp.StatusCode, string(body))
	}
	var info models.TopicInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return 0, err
	}
	return info.MessageCount, nil
}

func (s *Server) brokerRead(topic string, offset, limit int64) ([]models.Message, error) {
	base, err := s.brokerBaseURL()
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/topics/%s/messages?offset=%d&limit=%d", base, topic, offset, limit)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("broker read %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Messages []models.Message `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode: %v", err)
	}
}

func postJSON(client *http.Client, url string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return client.Post(url, "application/json", bytes.NewReader(b))
}

func parseLimit(q string, def int64) int64 {
	if q == "" {
		return def
	}
	n, err := strconv.ParseInt(q, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
