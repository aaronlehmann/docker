package xfer

import (
	"io"
)

// Progress represents the progress of a transfer.
type Progress struct {
	ID string

	// Progress contains a Message or...
	Message string

	// ...progress of an action
	Action  string
	Current int64
	Total   int64
}

// ProgressReader is a Reader with progress bar.
type ProgressReader struct {
	in         io.ReadCloser   // Stream to read from
	out        chan<- Progress // Where to send progress bar to
	size       int64
	current    int64
	lastUpdate int64
	id         string
	action     string
}

// NewProgressReader creates a new ProgressReader
func NewProgressReader(in io.ReadCloser, out chan<- Progress, size int64, id, action string) *ProgressReader {
	return &ProgressReader{
		in:     in,
		out:    out,
		size:   size,
		id:     id,
		action: action,
	}
}

func (p *ProgressReader) Read(buf []byte) (n int, err error) {
	read, err := p.in.Read(buf)
	p.current += int64(read)
	updateEvery := int64(1024 * 512) //512kB
	if p.size > 0 {
		// Update progress for every 1% read if 1% < 512kB
		if increment := int64(0.01 * float64(p.size)); increment < updateEvery {
			updateEvery = increment
		}
	}
	if p.current-p.lastUpdate > updateEvery || err != nil {
		p.updateProgress()
		p.lastUpdate = p.current
	}

	if err != nil && read == 0 {
		p.updateProgress()
	}
	return read, err
}

// Close closes the reader.
func (p *ProgressReader) Close() error {
	if p.current < p.size {
		// print a full progress bar when closing prematurely
		p.current = p.size
		p.updateProgress()
	}
	return p.in.Close()
}

func (p *ProgressReader) updateProgress() {
	p.out <- Progress{ID: p.id, Action: p.action, Current: p.current, Total: p.size}
}
