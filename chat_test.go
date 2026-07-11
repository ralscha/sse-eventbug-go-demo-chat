package main

import (
	"context"
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
	if response.Body.String() != "2" {
		t.Fatalf("client ID=%q", response.Body.String())
	}
	duplicate := httptest.NewRecorder()
	handler.ServeHTTP(duplicate, httptest.NewRequest(http.MethodPost, "/signin", strings.NewReader("alice")))
	if duplicate.Body.Len() != 0 {
		t.Fatal("duplicate nickname accepted")
	}
	added := httptest.NewRecorder()
	handler.ServeHTTP(added, httptest.NewRequest(http.MethodPost, "/addRoom", strings.NewReader("General")))
	if strings.TrimSpace(added.Body.String()) != "true" {
		t.Fatalf("add room=%s", added.Body.String())
	}
	rooms := httptest.NewRecorder()
	handler.ServeHTTP(rooms, httptest.NewRequest(http.MethodPost, "/subscribe", strings.NewReader("2")))
	if !strings.Contains(rooms.Body.String(), `"name":"General"`) {
		t.Fatalf("rooms=%s", rooms.Body.String())
	}
}

func TestRoomHistoryIsBounded(t *testing.T) {
	chat, _ := testChat(t)
	for range 110 {
		chat.store("2", message{Type: "MSG", SendDate: time.Now().UnixMilli()})
	}
	chat.mu.RLock()
	defer chat.mu.RUnlock()
	if len(chat.messages["2"]) != maxRoomMessages {
		t.Fatalf("history size=%d", len(chat.messages["2"]))
	}
}
