package main

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/johto/notifyutils/notifydispatcher"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

// Implements a wrapper for pq.Listener for use between the PostgreSQL server
// and NotifyDispatcher.  Here we pass the notifications on to the dispatcher.
type pqListenerWrapper struct {
	l  *pq.Listener
	ch chan *pq.Notification
}

func newPqListenerWrapper(l *pq.Listener) (*pqListenerWrapper, error) {
	w := &pqListenerWrapper{
		l:  l,
		ch: make(chan *pq.Notification, 4),
	}

	go w.workerGoroutine()
	return w, nil
}

func (w *pqListenerWrapper) workerGoroutine() {
	input := w.l.NotificationChannel()
	for {
		m := <-input
		w.ch <- m
	}
}

func (w *pqListenerWrapper) Listen(channel string) error {
	return w.l.Listen(channel)
}

func (w *pqListenerWrapper) Unlisten(channel string) error {
	return w.l.Unlisten(channel)
}

func (w *pqListenerWrapper) NotificationChannel() <-chan *pq.Notification {
	return w.ch
}

// runs in its own goroutine
func listenerPinger(listener *pq.Listener) {
	for {
		time.Sleep(60 * time.Second)
		_ = listener.Ping()
	}
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()

	cfg, err := LoadConfig()
	if err != nil {
		panic(err)
	}
	logger.Info("starting with config", zap.Any("config", cfg))

	var m sync.Mutex
	var connStatusNotifier chan struct{}

	listenerStateChange := func(ev pq.ListenerEventType, err error) {
		switch ev {
		case pq.ListenerEventConnectionAttemptFailed:
			logger.Warn("Listener: could not connect to the database", zap.Error(err))

		case pq.ListenerEventDisconnected:
			logger.Warn("Listener: lost connection to the database", zap.Error(err))
			m.Lock()
			defer m.Unlock()
			close(connStatusNotifier)
			connStatusNotifier = nil

		case pq.ListenerEventReconnected,
			pq.ListenerEventConnected:
			logger.Info("Listener: connected to the database")
			m.Lock()
			defer m.Unlock()
			connStatusNotifier = make(chan struct{})
		}
	}

	// make sure pq.Listener doesn't pick up any env variables
	// TODO: WhY=??
	os.Clearenv()

	listener := pq.NewListener(
		cfg.DatabaseAddr,
		250*time.Millisecond, 3*time.Second,
		listenerStateChange,
	)
	listenerWrapper, err := newPqListenerWrapper(listener)
	if err != nil {
		logger.Fatal("Could not create a pq listenerWrapper", zap.Error(err))
	}

	nd := notifydispatcher.NewNotifyDispatcher(listenerWrapper)
	nd.SetBroadcastOnConnectionLoss(false)
	nd.SetSlowReaderEliminationStrategy(notifydispatcher.NeglectSlowReaders)

	// We don't strictly speaking need to be pinging the server; this is a
	// workaround for PostgreSQL BUG #14830.
	go listenerPinger(listener)

	fc := NewServerEvent(logger, cfg, nd)
	fc.serve()
}
