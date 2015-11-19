package xfer

import (
	"errors"
	"fmt"
	"io"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/image"
	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/stringid"
	"golang.org/x/net/context"
)

type LayerDownloadManager struct {
	layerStore layer.Store
	tm         TransferManager
}

func NewLayerDownloadManager(layerStore layer.Store) *LayerDownloadManager {
	return &LayerDownloadManager{
		layerStore: layerStore,
		tm:         NewTransferManager(),
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

// Download is an interface returned by the download manager to allow getting
// the result of the nonblocking operation, and to release the resources
// associated with it.
type Download interface {
	Result() (image.RootFS, error)
	Release()
}

type returnedDownload struct {
	layerStore layer.Store
	layer      layer.Layer
	err        error

	topDownload  *downloadTransfer
	progressChan chan<- Progress // used by Release

	rootFS image.RootFS
}

// Release frees the resources associated with the download.
func (d *returnedDownload) Release() {
	if d.topDownload != nil {
		d.topDownload.Transfer.Release(d.progressChan)
	} else if d.layer != nil {
		layer.ReleaseAndLog(d.layerStore, d.layer)
	}
}

// Result returns the top layer for the image, if all layers were obtained
// successfully.
func (d *returnedDownload) Result() (image.RootFS, error) {
	return d.rootFS, d.err
}

// A Descriptor references a layer that may need to be downloaded.
type Descriptor interface {
	Key() string
	// Layer should return a reference to the layer, or an error if the
	// layer is not present (such as before calling Download).
	Layer() (layer.Layer, error)
	Download(ctx context.Context, progressChan chan<- Progress) (io.ReadCloser, int64, error)
	Registered(diffID layer.DiffID)
}

// Download is a non-blocking function which ensures the requested layers
// are present in the layer store. It uses the string returned by the
// Key method to deduplicate downloads. If a given layer is not already
// known to present in the layer store, and the key is not used by an
// in-progress download, the Download method is called to get the layer
// tar data. Layers are then registered in the appropriate order.
// Once the Progress channel is closed, the caller may call the Result
// method of the Download object. The caller must call the Release method
// of the Download object once it is finished with the returned layer.
// initialRootFS is generally an empty RootFS object, but may reference a base
// layer if applicable.
func (ldm *LayerDownloadManager) Download(ctx context.Context, initialRootFS image.RootFS, layers []Descriptor) (Download, chan Progress) {
	// Include a buffer so that slow client connections don't affect
	// transfer performance.
	progressChan := make(chan Progress, 100)
	returnedDownload := &returnedDownload{
		layerStore: ldm.layerStore,
	}

	go func() {
		defer func() {
			close(progressChan)
		}()

		var (
			topLayer    layer.Layer
			topDownload *downloadTransfer
		)

		transferKey := ""
		for _, descriptor := range layers {
			transferKey += descriptor.Key()

			l, err := descriptor.Layer()
			if err == nil {
				// Layer already exists.
				logrus.Debugf("Layer already exists: %s", descriptor.Key())
				progressChan <- progressMessage(descriptor, "Already exists")
				initialRootFS.Append(l.DiffID())
				if topLayer != nil {
					layer.ReleaseAndLog(ldm.layerStore, topLayer)
				}
				topLayer = l
				continue
			}

			// Layer is not known to exist - download and register it.
			progressChan <- progressMessage(descriptor, "Pulling fs layer")

			var xferFunc DoFunc
			if topDownload != nil {
				xferFunc = ldm.makeXferFunc(descriptor, "", topDownload)
				defer topDownload.Transfer.Release(progressChan)
			} else if topLayer != nil {
				xferFunc = ldm.makeXferFunc(descriptor, topLayer.ChainID(), nil)
			} else {
				xferFunc = ldm.makeXferFunc(descriptor, "", nil)
			}
			topDownload = ldm.tm.Transfer(transferKey, xferFunc, progressChan, topDownload).(*downloadTransfer)
		}

		if topDownload == nil {
			returnedDownload.layer = topLayer
			returnedDownload.rootFS = initialRootFS
			return
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
				returnedDownload.err = ctx.Err()
				return
			case <-topDownload.Done():
				break selectLoop
			}
		}

		l, err := topDownload.result()
		returnedDownload.err = err

		if err == nil {
			for l != nil {
				initialRootFS.DiffIDs = append([]layer.DiffID{l.DiffID()}, initialRootFS.DiffIDs...)
				l = l.Parent()
			}
			returnedDownload.rootFS = initialRootFS
		}

		returnedDownload.topDownload = topDownload
		returnedDownload.progressChan = progressChan
	}()

	return returnedDownload, progressChan
}

func (ldm *LayerDownloadManager) makeXferFunc(descriptor Descriptor, parentLayer layer.ChainID, parentDownload *downloadTransfer) DoFunc {
	return func(progressChan chan<- Progress) Transfer {
		d := &downloadTransfer{
			Transfer:   NewTransfer(),
			layerStore: ldm.layerStore,
		}

		go func() {
			defer func() {
				close(progressChan)
			}()

			downloadReader, size, err := descriptor.Download(d.Transfer.Context(), progressChan)
			if err != nil {
				d.err = err
				return
			}

			defer downloadReader.Close()

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

			reader := NewProgressReader(downloadReader, progressChan, size, stringid.TruncateID(descriptor.Key()), "Extracting")

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

			progressChan <- progressMessage(descriptor, "Pull complete")
			descriptor.Registered(d.layer.DiffID())

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

func progressMessage(descriptor Descriptor, message string) Progress {
	return Progress{ID: stringid.TruncateID(descriptor.Key()), Message: message}
}
