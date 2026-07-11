package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ralscha/sse-eventbus-go"
	"github.com/ralscha/sse-eventbus-go/httpadapter"
)

const (
	roomAddedEvent    = "roomAdded"
	roomsRemovedEvent = "roomsRemoved"
	messageRetention  = 6 * time.Hour
	maxRoomMessages   = 100
)

type room struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type message struct {
	Type     string `json:"type"`
	User     string `json:"user"`
	Message  string `json:"message"`
	SendDate int64  `json:"sendDate"`
}

type clientRequest struct {
	ClientID string `json:"clientId"`
	RoomID   string `json:"roomId"`
	Message  string `json:"message"`
}

type chatServer struct {
	bus          *sseeventbus.Bus
	mu           sync.RWMutex
	users        map[string]string
	rooms        map[string]room
	messages     map[string][]message
	nextClientID int64
	nextRoomID   int64
}

func newChatServer() *chatServer {
	return &chatServer{users: make(map[string]string), rooms: make(map[string]room), messages: make(map[string][]message), nextClientID: 1, nextRoomID: 1}
}

func (s *chatServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /signin", s.signin)
	mux.HandleFunc("POST /signinExisting", s.signinExisting)
	mux.HandleFunc("POST /signout", s.signout)
	mux.HandleFunc("POST /subscribe", s.subscribe)
	mux.HandleFunc("POST /addRoom", s.addRoom)
	mux.HandleFunc("POST /join", s.join)
	mux.HandleFunc("POST /leave", s.leave)
	mux.HandleFunc("POST /send", s.send)
	mux.HandleFunc("GET /register/{clientId}", s.register)
	if _, err := os.Stat("client/dist/app/browser"); err == nil {
		mux.Handle("/", http.FileServer(http.Dir("client/dist/app/browser")))
	}
	return mux
}

func (s *chatServer) signin(w http.ResponseWriter, r *http.Request) {
	nickname, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range s.users {
		if name == nickname {
			return
		}
	}
	s.nextClientID++
	id := strconv.FormatInt(s.nextClientID, 10)
	s.users[id] = nickname
	w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
	_, _ = io.WriteString(w, id)
}

func (s *chatServer) signinExisting(w http.ResponseWriter, r *http.Request) {
	nickname, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, name := range s.users {
		if name == nickname {
			_, _ = io.WriteString(w, id)
			return
		}
	}
	s.nextClientID++
	id := strconv.FormatInt(s.nextClientID, 10)
	s.users[id] = nickname
	_, _ = io.WriteString(w, id)
}

func (s *chatServer) signout(w http.ResponseWriter, r *http.Request) {
	id, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s.removeUser(id)
	s.bus.Unregister(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *chatServer) subscribe(w http.ResponseWriter, r *http.Request) {
	id, err := readText(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s.bus.Subscribe(id, roomAddedEvent, roomsRemovedEvent)
	s.mu.RLock()
	rooms := make([]room, 0, len(s.rooms))
	for _, value := range s.rooms {
		rooms = append(rooms, value)
	}
	s.mu.RUnlock()
	slices.SortFunc(rooms, func(a, b room) int { return strings.Compare(a.Name, b.Name) })
	writeJSON(w, rooms)
}

func (s *chatServer) addRoom(w http.ResponseWriter, r *http.Request) {
	name, err := readText(r)
	name = strings.TrimSpace(name)
	if err != nil || name == "" {
		writeJSON(w, false)
		return
	}
	s.mu.Lock()
	for _, existing := range s.rooms {
		if existing.Name == name {
			s.mu.Unlock()
			writeJSON(w, false)
			return
		}
	}
	s.nextRoomID++
	created := room{ID: strconv.FormatInt(s.nextRoomID, 10), Name: name}
	s.rooms[created.ID] = created
	s.mu.Unlock()
	s.publish(sseeventbus.NewNamedEventWithData(roomAddedEvent, created))
	writeJSON(w, true)
}

func (s *chatServer) join(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest(w, r)
	if !ok {
		return
	}
	user, exists := s.user(request.ClientID)
	if !exists {
		w.WriteHeader(204)
		return
	}
	joined := message{Type: "JOIN", User: user, Message: user + " has joined the room", SendDate: time.Now().UnixMilli()}
	history := s.store(request.RoomID, joined)
	s.bus.Subscribe(request.ClientID, request.RoomID)
	direct := sseeventbus.NewNamedEventWithData(request.RoomID, history)
	direct.ClientIDs = []string{request.ClientID}
	s.publish(direct)
	broadcast := sseeventbus.NewNamedEventWithData(request.RoomID, []message{joined})
	broadcast.ExcludeClientIDs = []string{request.ClientID}
	s.publish(broadcast)
	w.WriteHeader(204)
}

func (s *chatServer) leave(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest(w, r)
	if !ok {
		return
	}
	user, exists := s.user(request.ClientID)
	if !exists {
		w.WriteHeader(204)
		return
	}
	left := message{Type: "LEAVE", User: user, Message: user + " has left the room", SendDate: time.Now().UnixMilli()}
	s.store(request.RoomID, left)
	s.bus.Unsubscribe(request.ClientID, request.RoomID)
	s.publish(sseeventbus.NewNamedEventWithData(request.RoomID, []message{left}))
	w.WriteHeader(204)
}

func (s *chatServer) send(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest(w, r)
	if !ok {
		return
	}
	user, exists := s.user(request.ClientID)
	if !exists {
		w.WriteHeader(204)
		return
	}
	sent := message{Type: "MSG", User: user, Message: request.Message, SendDate: time.Now().UnixMilli()}
	s.store(request.RoomID, sent)
	s.publish(sseeventbus.NewNamedEventWithData(request.RoomID, []message{sent}))
	w.WriteHeader(204)
}

func (s *chatServer) register(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("clientId")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	if err := httpadapter.Serve(w, r, s.bus, id, httpadapter.WithTimeout(3*time.Minute)); err != nil && !errors.Is(err, sseeventbus.ErrClosed) {
		log.Printf("SSE client %q: %v", id, err)
	}
}

func (s *chatServer) user(id string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.users[id]
	return value, ok
}
func (s *chatServer) removeUser(id string) { s.mu.Lock(); delete(s.users, id); s.mu.Unlock() }
func (s *chatServer) publish(event sseeventbus.Event) {
	if err := s.bus.Publish(context.Background(), event); err != nil {
		log.Printf("publish %q: %v", event.Name, err)
	}
}

func (s *chatServer) store(roomID string, value message) []message {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Add(-messageRetention).UnixMilli()
	current := s.messages[roomID][:0]
	for _, item := range s.messages[roomID] {
		if item.SendDate >= now {
			current = append(current, item)
		}
	}
	current = append(current, value)
	if len(current) > maxRoomMessages {
		current = current[len(current)-maxRoomMessages:]
	}
	s.messages[roomID] = current
	return append([]message(nil), current...)
}

func (s *chatServer) cleanupRooms(ctx context.Context) {
	ticker := time.NewTicker(messageRetention)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.removeOldRooms()
		}
	}
}
func (s *chatServer) removeOldRooms() {
	cutoff := time.Now().Add(-messageRetention).UnixMilli()
	s.mu.Lock()
	var removed []string
	for id, items := range s.messages {
		kept := items[:0]
		for _, item := range items {
			if item.SendDate >= cutoff {
				kept = append(kept, item)
			}
		}
		if len(kept) == 0 {
			delete(s.messages, id)
			delete(s.rooms, id)
			removed = append(removed, id)
		} else {
			s.messages[id] = kept
		}
	}
	s.mu.Unlock()
	s.publish(sseeventbus.NewNamedEventWithData(roomsRemovedEvent, removed))
}

func readText(r *http.Request) (string, error) {
	defer func() { _ = r.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return string(data), err
}
func decodeRequest(w http.ResponseWriter, r *http.Request) (clientRequest, bool) {
	defer func() { _ = r.Body.Close() }()
	var request clientRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request); err != nil {
		http.Error(w, "invalid JSON", 400)
		return request, false
	}
	return request, true
}
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON: %v", err)
	}
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
