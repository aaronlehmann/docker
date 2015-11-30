package xfer

import (
	"sync"

	"golang.org/x/net/context"
)

// DoNotRetry is an error wrapper indicating that the error cannot be resolved
// with a retry.
type DoNotRetry struct {
	Err error
}

// Error returns the stringified representation of the encapsulated error.
func (e DoNotRetry) Error() string {
	return e.Err.Error()
}

// Watcher is returned by Watch and can be passed to Release to stop watching.
type Watcher struct {
	// signalChan is used to signal to the watcher goroutine that
	// new progress information is available, or that the transfer
	// has finished.
	signalChan chan struct{}
	// releaseChan signals to the watcher goroutine that the watcher
	// should be detached.
	releaseChan chan struct{}
	// running remains open as long as the watcher is watching the
	// transfer. It gets closed if the transfer finishes or the
	// watcher is detached.
	running chan struct{}
}

// Transfer represents an in-progress transfer.
type Transfer interface {
	Watch(progressChan chan<- Progress) *Watcher
	Release(*Watcher)
	Context() context.Context
	Cancel()
	Done() <-chan struct{}
	Released() <-chan struct{}
	Broadcast(masterProgressChan <-chan Progress)
}

type transfer struct {
	mu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	// watchers keeps track of the goroutines monitoring progress output,
	// indexed by the channels that release them.
	watchers map[chan struct{}]*Watcher

	// lastProgress is the most recently received progress event.
	lastProgress Progress
	// hasLastProgress is true when lastProgress has been set.
	hasLastProgress bool

	// running remains open as long as the transfer is in progress.
	running chan struct{}
	// hasWatchers stays open until all watchers release the trasnfer.
	hasWatchers chan struct{}

	// broadcastDone is true if the master progress channel has closed.
	broadcastDone bool
}

// NewTransfer creates a new transfer.
func NewTransfer() Transfer {
	t := &transfer{
		watchers:    make(map[chan struct{}]*Watcher),
		running:     make(chan struct{}),
		hasWatchers: make(chan struct{}),
	}

	// This uses context.Background instead of a caller-supplied context
	// so that a transfer won't be cancelled automatically if the client
	// which requested it is ^C'd (there could be other viewers).
	t.ctx, t.cancel = context.WithCancel(context.Background())

	return t
}

// Broadcast copies the progress and error output to all viewers.
func (t *transfer) Broadcast(masterProgressChan <-chan Progress) {
	for {
		p, ok := <-masterProgressChan
		t.mu.Lock()
		if ok {
			t.lastProgress = p
			t.hasLastProgress = true
			for _, w := range t.watchers {
				select {
				case w.signalChan <- struct{}{}:
				default:
				}
			}

		} else {
			t.broadcastDone = true
		}
		t.mu.Unlock()
		if !ok {
			close(t.running)
			return
		}
	}
}

// Watch adds a watcher to the transfer. The supplied channel gets progress
// updates and is closed when the transfer finishes.
func (t *transfer) Watch(progressChan chan<- Progress) *Watcher {
	t.mu.Lock()
	defer t.mu.Unlock()

	w := &Watcher{
		releaseChan: make(chan struct{}),
		signalChan:  make(chan struct{}),
		running:     make(chan struct{}),
	}

	if t.broadcastDone {
		close(w.running)
		return w
	}

	t.watchers[w.releaseChan] = w

	go func() {
		defer func() {
			close(w.running)
		}()
		done := false
		for {
			t.mu.Lock()
			hasLastProgress := t.hasLastProgress
			lastProgress := t.lastProgress
			t.mu.Unlock()

			// This might write the last progress item a
			// second time (since channel closure also gets
			// us here), but that's fine.
			if hasLastProgress {
				progressChan <- lastProgress
			}

			if done {
				return
			}

			select {
			case <-w.signalChan:
			case <-w.releaseChan:
				done = true
			case <-t.running:
				done = true
			}
		}
	}()

	return w
}

// Release is the inverse of Watch; indicating that the watcher no longer wants
// to be notified about the progress of the transfer. All calls to Watch must
// be paired with later calls to Release so that the lifecycle of the transfer
// is properly managed.
func (t *transfer) Release(watcher *Watcher) {
	t.mu.Lock()
	delete(t.watchers, watcher.releaseChan)

	if len(t.watchers) == 0 {
		close(t.hasWatchers)
		t.cancel()
	}
	t.mu.Unlock()

	close(watcher.releaseChan)
	// Block until the watcher goroutine completes
	<-watcher.running
}

// Done returns a channel which is closed if the transfer completes or is
// cancelled. Note that having 0 watchers causes a transfer to be cancelled.
func (t *transfer) Done() <-chan struct{} {
	// Note that this doesn't return t.ctx.Done() because that channel will
	// be closed the moment Cancel is called, and we need to return a
	// channel that blocks until a cancellation is actually acknowledged by
	// the transfer function.
	return t.running
}

// Released returns a channel which is closed once all watchers release the
// transfer.
func (t *transfer) Released() <-chan struct{} {
	return t.hasWatchers
}

// Context returns the context associated with the transfer.
func (t *transfer) Context() context.Context {
	return t.ctx
}

// Cancel cancels the context associated with the transfer.
func (t *transfer) Cancel() {
	t.cancel()
}

// DoFunc is a function called by the transfer manager to actually perform
// a transfer. It should be non-blocking. It should wait until the start channel
// is closed before transfering any data. If the function closes inactive, that
// signals to the transfer manager that the job is no longer actively moving
// data - for example, it may be waiting for a dependent tranfer to finish.
// This prevents it from taking up a slot.
type DoFunc func(progressChan chan<- Progress, start <-chan struct{}, inactive chan<- struct{}) Transfer

// TransferManager is used by LayerDownloadManager and LayerUploadManager to
// schedule and deduplicate transfers. It is up to the TransferManager
// implementation to make the scheduling and concurrency decisions.
type TransferManager interface {
	// Transfer checks if a transfer with the given key is in progress. If
	// so, it returns progress and error output from that transfer.
	// Otherwise, it will call xferFunc to initiate the transfer.
	Transfer(key string, xferFunc DoFunc, progressChan chan<- Progress) (Transfer, *Watcher)
}

type transferManager struct {
	mu sync.Mutex

	concurrencyLimit int
	activeTransfers  int
	transfers        map[string]Transfer
	waitingTransfers []chan struct{}
}

// NewTransferManager returns a new TransferManager.
func NewTransferManager(concurrencyLimit int) TransferManager {
	return &transferManager{
		concurrencyLimit: concurrencyLimit,
		transfers:        make(map[string]Transfer),
	}
}

// Transfer checks if a transfer matching the given key is in progress. If not,
// it starts one by calling xferFunc. The caller supplies a channel which
// receives progress output from the transfer.
func (tm *transferManager) Transfer(key string, xferFunc DoFunc, progressChan chan<- Progress) (Transfer, *Watcher) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if xfer, present := tm.transfers[key]; present {
		// Transfer is already in progress.
		watcher := xfer.Watch(progressChan)
		return xfer, watcher
	}

	start := make(chan struct{})
	inactive := make(chan struct{})

	if tm.activeTransfers < tm.concurrencyLimit {
		close(start)
		tm.activeTransfers++
	} else {
		tm.waitingTransfers = append(tm.waitingTransfers, start)
	}

	masterProgressChan := make(chan Progress)
	xfer := xferFunc(masterProgressChan, start, inactive)
	watcher := xfer.Watch(progressChan)
	go xfer.Broadcast(masterProgressChan)
	tm.transfers[key] = xfer

	// When the transfer is finished, remove from the map.
	go func() {
		for {
			select {
			case <-inactive:
				tm.mu.Lock()
				tm.inactivate(start)
				tm.mu.Unlock()
				inactive = nil
			case <-xfer.Done():
				tm.mu.Lock()
				if inactive != nil {
					tm.inactivate(start)
				}
				delete(tm.transfers, key)
				tm.mu.Unlock()
				return
			}
		}
	}()

	return xfer, watcher
}

func (tm *transferManager) inactivate(start chan struct{}) {
	// If the transfer was started, remove it from the activeTransfers
	// count.
	select {
	case <-start:
		// Start next transfer if any are waiting
		if len(tm.waitingTransfers) != 0 {
			close(tm.waitingTransfers[0])
			tm.waitingTransfers = tm.waitingTransfers[1:]
		} else {
			tm.activeTransfers--
		}
	default:
	}
}
