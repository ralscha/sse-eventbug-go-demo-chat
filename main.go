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
	bus, err := sseeventbus.New(
		sseeventbus.WithListener(&userLifecycleListener{onClientsRemoved: chat.removeUsers}),
		sseeventbus.WithHeartbeat(30*time.Second, "keep-alive"),
		sseeventbus.WithClientExpiration(time.Hour, time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
	chat.bus = bus

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go chat.cleanupRooms(ctx)

	server := &http.Server{Addr: ":8080", Handler: cors(chat.routes()), ReadHeaderTimeout: 5 * time.Second}
	serveErrors := make(chan error, 1)
	go func() {
		log.Printf("chat backend listening on http://localhost%s", server.Addr)
		serveErrors <- server.ListenAndServe()
	}()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serveErrors:
		stop()
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	if err := bus.Close(closeCtx); err != nil {
		log.Printf("close event bus: %v", err)
	}
	cancelClose()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shut down HTTP server: %v", err)
	}
	cancelShutdown()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		log.Fatal(serveErr)
	}
}
