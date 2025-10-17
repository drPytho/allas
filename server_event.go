package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/johto/notifyutils/notifydispatcher"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

var errClientCouldNotKeepUp = errors.New("client could not keep up")

type EventServer struct {
	// immutable
	logger *zap.Logger
	cfg    *Config

	dispatcher *notifydispatcher.NotifyDispatcher

	connStatusNotifier chan struct{}
	notify             chan *pq.Notification

	// owned by queryProcessingMainLoop until queryResultCh has been closed
	listenChannels map[string]struct{}

	lock sync.Mutex
	err  error
}

func NewEventServer(logger *zap.Logger, cfg *Config, dispatcher *notifydispatcher.NotifyDispatcher) *EventServer {
	fc := &EventServer{
		logger:     logger,
		cfg:        cfg,
		dispatcher: dispatcher,
	}
	return fc
}

func (c *EventServer) serve() {
	http.HandleFunc("/events", c.eventsHandler)
	c.logger.Info("Start listening...", zap.String("addr", c.cfg.BindAddr))
	http.ListenAndServe(c.cfg.BindAddr, nil)
}

type SubscriptionMessage struct {
	Channel string `json:"channel"`
	Payload string `json:"payload"`
}

func (c *EventServer) eventsHandler(w http.ResponseWriter, r *http.Request) {
	c.logger.Info("got a connection")
	channelsParam := r.URL.Query().Get("channels")
	channels := strings.Split(channelsParam, ",")

	// Set CORS headers to allow all origins. You may want to restrict this to specific origins in a production environment.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Type")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	defer c.logger.Info("Client disconnected")

	notifications := make(chan *pq.Notification, 256)
	// Clean up whitespace
	for i := range channels {
		channel := strings.TrimSpace(channels[i])
		if err := c.dispatcher.Listen(channel, notifications); err != nil {
			c.logger.Error("could not listen to channel", zap.String("channel", channel), zap.Error(err))
			return
		}
		defer c.dispatcher.Unlisten(channel, notifications)
	}

	// Simulate sending events (you can replace this with real data)
	for n := range notifications {
		if len(notifications) >= cap(notifications)-1 {
			c.logger.Error("Client could not keep up", zap.Error(errClientCouldNotKeepUp))
			return
		}

		if n == nil {
			c.logger.Info("lost connection, but we're fine now!")
			continue
		}
		c.logger.Info("sending message", zap.Any("msg", n))
		sm := SubscriptionMessage{
			Channel: n.Channel,
			Payload: n.Extra,
		}

		// do something with notification
		if err := sendEvent(w, sm); err != nil {
			c.logger.Error("Could not marshal payload", zap.Error(err))
		}

	}
}

func sendEvent(w http.ResponseWriter, sm SubscriptionMessage) error {
	defer w.(http.Flusher).Flush()

	jsonData, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", jsonData); err != nil {
		return err
	}
	return nil
}
