package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ralscha/sse-eventbus-go"
)

func testChat(t *testing.T) (*chatServer, http.Handler) {
	t.Helper()
	chat := newChatServer()
	bus, err := sseeventbus.New(sseeventbus.WithSynchronousDelivery())
	if err != nil {
		t.Fatal(err)
	}
	chat.bus = bus
	t.Cleanup(func() { _ = bus.Close(context.Background()) })
	return chat, cors(chat.routes())
}

func TestSigninRoomAndSubscribe(t *testing.T) {
	_, handler := testChat(t)
	request := httptest.NewRequest(http.MethodPost, "/signin", strings.NewReader("alice"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Body.String() != "1" {
		t.Fatalf("client ID=%q", response.Body.String())
	}
	duplicate := httptest.NewRecorder()
	handler.ServeHTTP(duplicate, httptest.NewRequest(http.MethodPost, "/signin", strings.NewReader("alice")))
	if duplicate.Code != http.StatusConflict {
		t.Fatal("duplicate nickname accepted")
	}
	added := httptest.NewRecorder()
	handler.ServeHTTP(added, httptest.NewRequest(http.MethodPost, "/addRoom", strings.NewReader("General")))
	if strings.TrimSpace(added.Body.String()) != "true" {
		t.Fatalf("add room=%s", added.Body.String())
	}
	rooms := httptest.NewRecorder()
	handler.ServeHTTP(rooms, httptest.NewRequest(http.MethodGet, "/rooms", nil))
	if !strings.Contains(rooms.Body.String(), `"name":"General"`) {
		t.Fatalf("rooms=%s", rooms.Body.String())
	}
}

func TestSigninRejectsBlankNickname(t *testing.T) {
	_, handler := testChat(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/signin", strings.NewReader("  ")))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestJoinRejectsUnknownRoom(t *testing.T) {
	_, handler := testChat(t)
	signin := httptest.NewRecorder()
	handler.ServeHTTP(signin, httptest.NewRequest(http.MethodPost, "/signin", strings.NewReader("alice")))

	response := httptest.NewRecorder()
	body := `{"clientId":"1","roomId":"missing"}`
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/join", strings.NewReader(body)))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestRoomHistoryIsBounded(t *testing.T) {
	chat, _ := testChat(t)
	chat.rooms["2"] = room{ID: "2", Name: "General"}
	for range 110 {
		if _, ok := chat.store("2", message{Type: "MSG", SendDate: time.Now().UnixMilli()}); !ok {
			t.Fatal("room disappeared")
		}
	}
	chat.mu.RLock()
	defer chat.mu.RUnlock()
	if len(chat.messages["2"]) != maxRoomMessages {
		t.Fatalf("history size=%d", len(chat.messages["2"]))
	}
}

func TestSendRequiresRoomMembership(t *testing.T) {
	_, handler := testChat(t)
	for path, body := range map[string]string{"/signin": "alice", "/addRoom": "General"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	}
	response := httptest.NewRecorder()
	body := `{"clientId":"1","roomId":"1","message":"hello"}`
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(body)))
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestCleanupRemovesEmptyExpiredRooms(t *testing.T) {
	chat, _ := testChat(t)
	chat.rooms["old"] = room{ID: "old", Name: "Old"}
	chat.roomActivity["old"] = time.Now().Add(-messageRetention - time.Minute).UnixMilli()

	chat.removeOldRooms(context.Background())
	if chat.hasRoom("old") {
		t.Fatal("expired empty room was retained")
	}
}

func TestDisconnectPreservesChatSession(t *testing.T) {
	chat, handler := testChat(t)
	chat.users["1"] = "alice"
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		close(done)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/register/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	initial := make([]byte, len(":\n\n"))
	if _, err := io.ReadFull(response.Body, initial); err != nil || string(initial) != ":\n\n" {
		t.Fatalf("initial SSE frame = %q, err = %v", initial, err)
	}
	_ = response.Body.Close()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not stop after disconnect")
	}

	if !chat.bus.IsClientRegistered("1") || chat.bus.CountSubscribers(roomAddedEvent) != 1 {
		t.Fatal("disconnect removed the logical chat session")
	}
	if err := chat.bus.Publish(context.Background(), sseeventbus.NewNamedEventWithData(roomAddedEvent, room{ID: "1"})); err != nil {
		t.Fatalf("offline publish = %v", err)
	}
}
