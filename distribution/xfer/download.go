package xfer

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/image"
	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/archive"
	"golang.org/x/net/context"
)

const maxDownloadAttempts = 5

// LayerDownloadManager figures out which layers need to be downloaded, then
// registers and downloads those, taking into account dependencies between
// layers.
type LayerDownloadManager struct {
	layerStore layer.Store
	tm         TransferManager
}

// NewLayerDownloadManager returns a new LayerDownloadManager.
func NewLayerDownloadManager(layerStore layer.Store, concurrencyLimit int) *LayerDownloadManager {
	return &LayerDownloadManager{
		layerStore: layerStore,
		tm:         NewTransferManager(concurrencyLimit),
	}
}

type downloadTransfer struct {
	Transfer

	layerStore layer.Store
	layer      layer.Layer
	err        error
}

// result returns the layer resulting from the download, if the download
// and registration were successful.
func (d *downloadTransfer) result() (layer.Layer, error) {
	return d.layer, d.err
}

// A DownloadDescriptor references a layer that may need to be downloaded.
type DownloadDescriptor interface {
	// Key returns the key used to deduplicate downloads.
	Key() string
	// ID returns the ID for display purposes.
	ID() string
	// DiffID should return the DiffID for this layer, or an error
	// if it is unknown (for example, if it has not been downloaded
	// before).
	DiffID() (layer.DiffID, error)
	// Download is called to perform the download.
	Download(ctx context.Context, progressChan chan<- Progress) (io.ReadCloser, int64, error)
}

// DownloadDescriptorWithRegistered is a DownloadDescriptor that has an
// additional Registered method which gets called after a downloaded layer is
// registered. This allows the user of the download manager to know the DiffID
// of each registered layer. This method is called if a cast to
// DownloadDescriptorWithRegistered is successful.
type DownloadDescriptorWithRegistered interface {
	DownloadDescriptor
	Registered(diffID layer.DiffID)
}

// Download is a blocking function which ensures the requested layers are
// present in the layer store. It uses the string returned by the Key method to
// deduplicate downloads. If a given layer is not already known to present in
// the layer store, and the key is not used by an in-progress download, the
// Download method is called to get the layer tar data. Layers are then
// registered in the appropriate order.  The caller must call the returned
// release function once it is is done with the returned RootFS object.
func (ldm *LayerDownloadManager) Download(ctx context.Context, initialRootFS image.RootFS, layers []DownloadDescriptor, progressChan chan<- Progress) (image.RootFS, func(), error) {
	var (
		topLayer     layer.Layer
		topDownload  *downloadTransfer
		watcher      *Watcher
		missingLayer bool
	)

	transferKey := ""
	for _, descriptor := range layers {
		transferKey += descriptor.Key()

		if !missingLayer {
			missingLayer = true
			diffID, err := descriptor.DiffID()
			if err == nil {
				initialRootFS.Append(diffID)
				l, err := ldm.layerStore.Get(initialRootFS.ChainID())
				if err == nil {
					// Layer already exists.
					logrus.Debugf("Layer already exists: %s", descriptor.ID())
					progressChan <- downloadMessage(descriptor, "Already exists")
					if topLayer != nil {
						layer.ReleaseAndLog(ldm.layerStore, topLayer)
					}
					topLayer = l
					missingLayer = false
					continue
				}
			}
		}

		// Layer is not known to exist - download and register it.
		progressChan <- downloadMessage(descriptor, "Pulling fs layer")

		var xferFunc DoFunc
		if topDownload != nil {
			xferFunc = ldm.makeDownloadFunc(descriptor, "", topDownload)
			defer topDownload.Transfer.Release(watcher)
		} else if topLayer != nil {
			xferFunc = ldm.makeDownloadFunc(descriptor, topLayer.ChainID(), nil)
		} else {
			xferFunc = ldm.makeDownloadFunc(descriptor, "", nil)
		}
		var topDownloadUncasted Transfer
		topDownloadUncasted, watcher = ldm.tm.Transfer(transferKey, xferFunc, progressChan, topDownload)
		topDownload = topDownloadUncasted.(*downloadTransfer)
	}

	if topDownload == nil {
		return initialRootFS, func() { layer.ReleaseAndLog(ldm.layerStore, topLayer) }, nil
	}

	// Won't be using the list built up so far - will generate it
	// from downloaded layers instead.
	initialRootFS.DiffIDs = []layer.DiffID{}

	defer func() {
		if topLayer != nil {
			layer.ReleaseAndLog(ldm.layerStore, topLayer)
		}
	}()

selectLoop:
	for {
		select {
		case <-ctx.Done():
			topDownload.Transfer.Release(watcher)
			return initialRootFS, func() {}, ctx.Err()
		case <-topDownload.Done():
			break selectLoop
		}
	}

	l, err := topDownload.result()
	if err != nil {
		topDownload.Transfer.Release(watcher)
		return initialRootFS, func() {}, err
	}

	for l != nil {
		initialRootFS.DiffIDs = append([]layer.DiffID{l.DiffID()}, initialRootFS.DiffIDs...)
		l = l.Parent()
	}
	return initialRootFS, func() { topDownload.Transfer.Release(watcher) }, err
}

func (ldm *LayerDownloadManager) makeDownloadFunc(descriptor DownloadDescriptor, parentLayer layer.ChainID, parentDownload *downloadTransfer) DoFunc {
	return func(progressChan chan<- Progress, start <-chan struct{}, inactive chan<- struct{}) Transfer {
		d := &downloadTransfer{
			Transfer:   NewTransfer(),
			layerStore: ldm.layerStore,
		}

		go func() {
			defer func() {
				close(progressChan)
			}()

			select {
			case <-start:
			default:
				progressChan <- downloadMessage(descriptor, "Waiting")
				<-start
			}

			if parentDownload != nil {
				// Did the parent download already fail or get
				// cancelled?
				select {
				case <-parentDownload.Done():
					_, err := parentDownload.result()
					if err != nil {
						d.err = err
						return
					}
				default:
				}
			}

			var (
				downloadReader io.ReadCloser
				size           int64
				err            error
				retries        int
			)

			for {
				downloadReader, size, err = descriptor.Download(d.Transfer.Context(), progressChan)
				if err == nil {
					break
				}
				retries++
				if _, isDNR := err.(DoNotRetry); isDNR || retries == maxDownloadAttempts {
					d.err = err
					return
				}

				delay := retries * 5
				ticker := time.NewTicker(time.Second)
				for {
					progressChan <- downloadMessage(descriptor, fmt.Sprintf("Retrying in %d seconds", delay))
					delay--
					if delay == 0 {
						ticker.Stop()
						break
					}
					<-ticker.C
				}
			}

			defer downloadReader.Close()

			close(inactive)

			if parentDownload != nil {
				select {
				case <-d.Transfer.Done():
					d.err = errors.New("layer registration cancelled")
					return
				case <-parentDownload.Done():
				}

				l, err := parentDownload.result()
				if err != nil {
					d.err = err
					return
				}
				parentLayer = l.ChainID()
			}

			reader := NewProgressReader(downloadReader, progressChan, size, descriptor.ID(), "Extracting")

			inflatedLayerData, err := archive.DecompressStream(reader)
			if err != nil {
				d.err = fmt.Errorf("could not get decompression stream: %v", err)
				return
			}

			registerDone := make(chan struct{})

			go func() {
				select {
				case <-d.Transfer.Done():
					inflatedLayerData.Close()
				case <-registerDone:
				}
			}()

			d.layer, err = d.layerStore.Register(inflatedLayerData, parentLayer)
			close(registerDone)
			if err != nil {
				select {
				case <-d.Transfer.Done():
					d.err = errors.New("layer registration cancelled")
				default:
					d.err = fmt.Errorf("failed to register layer: %v", err)
				}
				return
			}

			progressChan <- downloadMessage(descriptor, "Pull complete")
			withRegistered, hasRegistered := descriptor.(DownloadDescriptorWithRegistered)
			if hasRegistered {
				withRegistered.Registered(d.layer.DiffID())
			}

			// Doesn't actually need to be its own goroutine, but
			// done like this so we can defer close(c).
			go func() {
				<-d.Transfer.Released()
				if d.layer != nil {
					layer.ReleaseAndLog(d.layerStore, d.layer)
				}
			}()
		}()

		return d
	}
}

func downloadMessage(descriptor DownloadDescriptor, message string) Progress {
	return Progress{ID: descriptor.ID(), Action: message}
}
