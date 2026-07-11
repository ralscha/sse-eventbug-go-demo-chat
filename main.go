package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ralscha/sse-eventbus-go"
)

func main() {
	chat := newChatServer()
	registry := &userRegistry{MemorySubscriptionRegistry: sseeventbus.NewMemorySubscriptionRegistry(), onClientRemoved: chat.removeUser}
	bus, err := sseeventbus.New(
		sseeventbus.WithSubscriptionRegistry(registry),
		sseeventbus.WithClientExpiration(time.Hour, time.Hour),
	)
	if err != nil {
		log.Fatal(err)
	}
	chat.bus = bus

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go chat.cleanupRooms(ctx)

	server := &http.Server{Addr: ":8080", Handler: cors(chat.routes()), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("chat backend listening on http://localhost%s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = bus.Close(shutdownCtx)
}
