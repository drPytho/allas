package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/johto/notifyutils/notifydispatcher"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

var (
	errGracefulTermination  = errors.New("graceful termination")
	errClientCouldNotKeepUp = errors.New("client could not keep up")
	errLostServerConnection = errors.New("lost server connection")
)

type FrontendConnection struct {
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

func NewServerEvent(logger *zap.Logger, cfg *Config, dispatcher *notifydispatcher.NotifyDispatcher) *FrontendConnection {
	fc := &FrontendConnection{
		logger:     logger,
		cfg:        cfg,
		dispatcher: dispatcher,
	}
	return fc
}

func (c *FrontendConnection) serve() {
	http.HandleFunc("/events", c.eventsHandler)
	c.logger.Info("Start listening...", zap.String("addr", c.cfg.BindAddr))
	http.ListenAndServe(c.cfg.BindAddr, nil)
}

func (c *FrontendConnection) eventsHandler(w http.ResponseWriter, r *http.Request) {
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

	ch := make(chan *pq.Notification, 256)
	// Clean up whitespace
	for i := range channels {
		channel := strings.TrimSpace(channels[i])
		if err := c.dispatcher.Listen(channel, ch); err != nil {
			panic(err)
		}
		defer c.dispatcher.Unlisten(channel, ch)
	}

	// Simulate sending events (you can replace this with real data)
	for n := range ch {
		if len(ch) >= cap(ch)-1 {
			c.logger.Error("Clinet could not keep up", zap.Error(errClientCouldNotKeepUp))
			return
		}

		if n == nil {
			c.logger.Info("lost connection, but we're fine now!")
			continue
		}
		c.logger.Info("sendign message")

		// do something with notification
		fmt.Fprintf(w, "data: %s\n\n", fmt.Sprintf("Event %d", n.Extra))
		w.(http.Flusher).Flush()
	}
}
