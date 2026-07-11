package main

import "github.com/ralscha/sse-eventbus-go"

type userRegistry struct {
	*sseeventbus.MemorySubscriptionRegistry
	onClientRemoved func(string)
}

func (r *userRegistry) Unsubscribe(clientID, event string) {
	r.MemorySubscriptionRegistry.Unsubscribe(clientID, event)
	if event == roomAddedEvent {
		r.onClientRemoved(clientID)
	}
}
func (r *userRegistry) UnsubscribeAll(clientID string) {
	wasRegistered := r.IsSubscribed(clientID, roomAddedEvent)
	r.MemorySubscriptionRegistry.UnsubscribeAll(clientID)
	if wasRegistered {
		r.onClientRemoved(clientID)
	}
}
