package main

import "github.com/ralscha/sse-eventbus-go"

// userLifecycleListener removes application state when the bus automatically
// retires inactive or unreachable clients. Explicit sign-out does this itself.
type userLifecycleListener struct {
	sseeventbus.NopListener
	onClientsRemoved func([]string)
}

func (l *userLifecycleListener) AfterClientsUnregistered(clientIDs []string) {
	l.onClientsRemoved(clientIDs)
}
