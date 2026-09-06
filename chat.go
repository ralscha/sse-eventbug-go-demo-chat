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
	maxRequestBody    = 1 << 20
	sseWriteTimeout   = 10 * time.Second
)

var errRequestTooLarge = errors.New("request body is too large")

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
	roomActivity map[string]int64
	nextClientID int64
	nextRoomID   int64
}

func newChatServer() *chatServer {
	return &chatServer{
		users: make(map[string]string), rooms: make(map[string]room),
		messages: make(map[string][]message), roomActivity: make(map[string]int64),
	}
}

func (s *chatServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /signin", s.signin)
	mux.HandleFunc("POST /signinExisting", s.signinExisting)
	mux.HandleFunc("POST /signout", s.signout)
	mux.HandleFunc("GET /rooms", s.listRooms)
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
	s.signinUser(w, r, false)
}

func (s *chatServer) signinExisting(w http.ResponseWriter, r *http.Request) {
	s.signinUser(w, r, true)
}

func (s *chatServer) signinUser(w http.ResponseWriter, r *http.Request, allowExisting bool) {
	nickname, err := readText(r)
	if err != nil {
		writeRequestError(w, err)
		return
	}
	nickname = strings.TrimSpace(nickname)
	if nickname == "" {
		http.Error(w, "nickname is required", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	for id, name := range s.users {
		if name == nickname {
			if !allowExisting {
				s.mu.Unlock()
				w.WriteHeader(http.StatusConflict)
				return
			}
			s.mu.Unlock()
			w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
			_, _ = io.WriteString(w, id)
			return
		}
	}
	s.nextClientID++
	id := strconv.FormatInt(s.nextClientID, 10)
	s.users[id] = nickname
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
	_, _ = io.WriteString(w, id)
}

func (s *chatServer) signout(w http.ResponseWriter, r *http.Request) {
	id, err := readText(r)
	if err != nil {
		writeRequestError(w, err)
		return
	}
	id = strings.TrimSpace(id)
	s.removeUser(id)
	s.bus.Unregister(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *chatServer) listRooms(w http.ResponseWriter, _ *http.Request) {
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
	s.roomActivity[created.ID] = time.Now().UnixMilli()
	s.mu.Unlock()
	s.publish(r.Context(), sseeventbus.NewNamedEventWithData(roomAddedEvent, created))
	writeJSON(w, true)
}

func (s *chatServer) join(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest(w, r)
	if !ok {
		return
	}
	user, exists := s.user(request.ClientID)
	if !exists {
		http.Error(w, "unknown client", http.StatusUnauthorized)
		return
	}
	if !s.hasRoom(request.RoomID) {
		http.Error(w, "unknown room", http.StatusNotFound)
		return
	}
	if s.isRoomMember(request.ClientID, request.RoomID) {
		history, exists := s.roomHistory(request.RoomID)
		if !exists {
			http.Error(w, "unknown room", http.StatusNotFound)
			return
		}
		direct := sseeventbus.NewNamedEventWithData(request.RoomID, history)
		direct.ClientIDs = []string{request.ClientID}
		s.publish(r.Context(), direct)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	joined := message{Type: "JOIN", User: user, Message: user + " has joined the room", SendDate: time.Now().UnixMilli()}
	history, exists := s.store(request.RoomID, joined)
	if !exists {
		http.Error(w, "unknown room", http.StatusNotFound)
		return
	}
	s.bus.Subscribe(request.ClientID, request.RoomID)
	direct := sseeventbus.NewNamedEventWithData(request.RoomID, history)
	direct.ClientIDs = []string{request.ClientID}
	s.publish(r.Context(), direct)
	broadcast := sseeventbus.NewNamedEventWithData(request.RoomID, []message{joined})
	broadcast.ExcludeClientIDs = []string{request.ClientID}
	s.publish(r.Context(), broadcast)
	w.WriteHeader(http.StatusNoContent)
}

func (s *chatServer) leave(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest(w, r)
	if !ok {
		return
	}
	user, exists := s.user(request.ClientID)
	if !exists {
		http.Error(w, "unknown client", http.StatusUnauthorized)
		return
	}
	if !s.isRoomMember(request.ClientID, request.RoomID) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	left := message{Type: "LEAVE", User: user, Message: user + " has left the room", SendDate: time.Now().UnixMilli()}
	if _, exists := s.store(request.RoomID, left); !exists {
		s.bus.Unsubscribe(request.ClientID, request.RoomID)
		http.Error(w, "unknown room", http.StatusNotFound)
		return
	}
	s.bus.Unsubscribe(request.ClientID, request.RoomID)
	s.publish(r.Context(), sseeventbus.NewNamedEventWithData(request.RoomID, []message{left}))
	w.WriteHeader(http.StatusNoContent)
}

func (s *chatServer) send(w http.ResponseWriter, r *http.Request) {
	request, ok := decodeRequest(w, r)
	if !ok {
		return
	}
	user, exists := s.user(request.ClientID)
	if !exists {
		http.Error(w, "unknown client", http.StatusUnauthorized)
		return
	}
	if !s.hasRoom(request.RoomID) {
		http.Error(w, "unknown room", http.StatusNotFound)
		return
	}
	if !s.isRoomMember(request.ClientID, request.RoomID) {
		http.Error(w, "join the room before sending messages", http.StatusForbidden)
		return
	}
	request.Message = strings.TrimSpace(request.Message)
	if request.Message == "" {
		http.Error(w, "message is required", http.StatusBadRequest)
		return
	}
	sent := message{Type: "MSG", User: user, Message: request.Message, SendDate: time.Now().UnixMilli()}
	if _, exists := s.store(request.RoomID, sent); !exists {
		http.Error(w, "unknown room", http.StatusNotFound)
		return
	}
	s.publish(r.Context(), sseeventbus.NewNamedEventWithData(request.RoomID, []message{sent}))
	w.WriteHeader(http.StatusNoContent)
}

func (s *chatServer) register(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("clientId"))
	if id == "" || len(id) > 128 {
		http.Error(w, "invalid client ID", http.StatusBadRequest)
		return
	}
	if _, exists := s.user(id); !exists {
		http.Error(w, "unknown client", http.StatusNotFound)
		return
	}
	if err := httpadapter.Serve(w, r, s.bus, id,
		httpadapter.WithTimeout(0),
		httpadapter.WithWriteTimeout(sseWriteTimeout),
		httpadapter.WithRegistration(sseeventbus.SubscribeTo(roomAddedEvent, roomsRemovedEvent)),
	); err != nil && !errors.Is(err, sseeventbus.ErrClosed) && !errors.Is(err, context.Canceled) {
		log.Printf("SSE client %q: %v", id, err)
	}
}

func (s *chatServer) user(id string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.users[id]
	return value, ok
}
func (s *chatServer) hasRoom(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.rooms[id]
	return ok
}
func (s *chatServer) isRoomMember(clientID, roomID string) bool {
	return slices.Contains(s.bus.Subscribers(roomID), clientID)
}
func (s *chatServer) removeUser(id string) { s.mu.Lock(); delete(s.users, id); s.mu.Unlock() }
func (s *chatServer) removeUsers(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.users, id)
	}
}
func (s *chatServer) publish(ctx context.Context, event sseeventbus.Event) {
	if err := s.bus.Publish(ctx, event); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("publish %q: %v", event.Name, err)
	}
}

func (s *chatServer) roomHistory(roomID string) ([]message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, exists := s.rooms[roomID]; !exists {
		return nil, false
	}
	cutoff := time.Now().Add(-messageRetention).UnixMilli()
	history := make([]message, 0, len(s.messages[roomID]))
	for _, item := range s.messages[roomID] {
		if item.SendDate >= cutoff {
			history = append(history, item)
		}
	}
	return history, true
}

func (s *chatServer) store(roomID string, value message) ([]message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rooms[roomID]; !exists {
		return nil, false
	}
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
	s.roomActivity[roomID] = value.SendDate
	return append([]message(nil), current...), true
}

func (s *chatServer) cleanupRooms(ctx context.Context) {
	ticker := time.NewTicker(messageRetention)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.removeOldRooms(ctx)
		}
	}
}
func (s *chatServer) removeOldRooms(ctx context.Context) {
	cutoff := time.Now().Add(-messageRetention).UnixMilli()
	s.mu.Lock()
	var removed []string
	for id := range s.rooms {
		items := s.messages[id]
		kept := items[:0]
		for _, item := range items {
			if item.SendDate >= cutoff {
				kept = append(kept, item)
			}
		}
		clear(items[len(kept):])
		if len(kept) == 0 && s.roomActivity[id] < cutoff {
			delete(s.messages, id)
			delete(s.rooms, id)
			delete(s.roomActivity, id)
			removed = append(removed, id)
		} else {
			s.messages[id] = kept
		}
	}
	s.mu.Unlock()
	if len(removed) == 0 {
		return
	}
	slices.Sort(removed)
	for _, roomID := range removed {
		for _, clientID := range s.bus.Subscribers(roomID) {
			s.bus.Unsubscribe(clientID, roomID)
		}
	}
	s.publish(ctx, sseeventbus.NewNamedEventWithData(roomsRemovedEvent, removed))
}

func readText(r *http.Request) (string, error) {
	defer func() { _ = r.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if len(data) > maxRequestBody {
		return "", errRequestTooLarge
	}
	return string(data), err
}
func decodeRequest(w http.ResponseWriter, r *http.Request) (clientRequest, bool) {
	defer func() { _ = r.Body.Close() }()
	var request clientRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBody+1))
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return request, false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request must contain one JSON object", http.StatusBadRequest)
		return request, false
	}
	request.ClientID = strings.TrimSpace(request.ClientID)
	request.RoomID = strings.TrimSpace(request.RoomID)
	return request, true
}

func writeRequestError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, errRequestTooLarge) {
		status = http.StatusRequestEntityTooLarge
	}
	http.Error(w, err.Error(), status)
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
