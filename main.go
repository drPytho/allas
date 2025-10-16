package main

import (
	"github.com/johto/notifyutils/notifydispatcher"
	"github.com/lib/pq"

	"fmt"
	"os"
	"sync"
	"time"
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

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  %s [--help] configfile

Options:
  --help                display this help and exit
`, os.Args[0])
}

func main() {
	InitErrorLog(os.Stderr)

	if len(os.Args) != 2 {
		printUsage()
		os.Exit(1)
	} else if os.Args[1] == "--help" {
		printUsage()
		os.Exit(1)
	}

	err := readConfigFile(os.Args[1])
	if err != nil {
		elog.Fatalf("error while reading configuration file: %s", err)
	}
	if len(Config.Databases) == 0 {
		elog.Fatalf("at least one database must be configured")
	}

	l, err := Config.Listen.Listen()
	if err != nil {
		elog.Fatalf("could not open listen socket: %s", err)
	}

	var m sync.Mutex
	var connStatusNotifier chan struct{}

	listenerStateChange := func(ev pq.ListenerEventType, err error) {
		switch ev {
		case pq.ListenerEventConnectionAttemptFailed:
			elog.Warningf("Listener: could not connect to the database: %s", err.Error())

		case pq.ListenerEventDisconnected:
			elog.Warningf("Listener: lost connection to the database: %s", err.Error())
			m.Lock()
			close(connStatusNotifier)
			connStatusNotifier = nil
			m.Unlock()

		case pq.ListenerEventReconnected,
			pq.ListenerEventConnected:
			elog.Logf("Listener: connected to the database")
			m.Lock()
			connStatusNotifier = make(chan struct{})
			m.Unlock()
		}
	}

	// make sure pq.Listener doesn't pick up any env variables
	os.Clearenv()

	clientConnectionString := fmt.Sprintf("fallback_application_name=allas %s", Config.ClientConnInfo)
	listener := pq.NewListener(
		clientConnectionString,
		250*time.Millisecond, 3*time.Second,
		listenerStateChange,
	)
	listenerWrapper, err := newPqListenerWrapper(listener)
	if err != nil {
		elog.Fatalf("%s", err)
	}
	nd := notifydispatcher.NewNotifyDispatcher(listenerWrapper)
	nd.SetBroadcastOnConnectionLoss(false)
	nd.SetSlowReaderEliminationStrategy(notifydispatcher.NeglectSlowReaders)

	// We don't strictly speaking need to be pinging the server; this is a
	// workaround for PostgreSQL BUG #14830.
	go listenerPinger(listener)

	for {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}

		Config.Listen.MaybeEnableKeepAlive(c)

		var myConnStatusNotifier chan struct{}

		m.Lock()
		if connStatusNotifier == nil {
			m.Unlock()
			go RejectFrontendConnection(c)
			continue
		} else {
			myConnStatusNotifier = connStatusNotifier
		}
		m.Unlock()

		newConn := NewFrontendConnection(c, nd, myConnStatusNotifier)
		go newConn.mainLoop(Config.StartupParameters, Config.Databases)
	}
}
