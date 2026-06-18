package broker

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/pert/message-broker/pkg/models"
)

type Server struct {
	storage *Storage
	mux     *http.ServeMux
}

func NewServer(storage *Storage) *Server {
	s := &Server{storage: storage, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /topics", s.handleListTopics)
	s.mux.HandleFunc("GET /topics/{topic}", s.handleTopicInfo)
	s.mux.HandleFunc("POST /topics/{topic}/messages", s.handlePublish)
	s.mux.HandleFunc("GET /topics/{topic}/messages", s.handleRead)
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"topics": s.storage.Topics()})
}

func (s *Server) handleTopicInfo(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		http.Error(w, "topic required", http.StatusBadRequest)
		return
	}
	info := models.TopicInfo{
		Topic:        topic,
		MessageCount: s.storage.Count(topic),
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		http.Error(w, "topic required", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	payload := strings.TrimSpace(string(body))
	if payload == "" {
		var req models.PublishRequest
		if err := json.Unmarshal(body, &req); err == nil && req.Payload != "" {
			payload = req.Payload
		}
	}
	if payload == "" {
		http.Error(w, "empty payload", http.StatusBadRequest)
		return
	}

	msg, err := s.storage.Append(topic, payload)
	if err != nil {
		log.Printf("append error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, msg)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		http.Error(w, "topic required", http.StatusBadRequest)
		return
	}

	offset := int64(0)
	if v := r.URL.Query().Get("offset"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
		offset = n
	}
	limit := int64(100)
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = n
	}

	msgs, err := s.storage.Read(topic, offset, limit)
	if err != nil {
		log.Printf("read error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode error: %v", err)
	}
}
