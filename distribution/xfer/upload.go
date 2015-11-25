package xfer

import (
	"fmt"
	"time"

	"github.com/docker/distribution/digest"
	"github.com/docker/docker/layer"
	"golang.org/x/net/context"
)

const maxUploadAttempts = 5

// LayerUploadManager provides task management and progress reporting for
// uploads.
type LayerUploadManager struct {
	tm TransferManager
}

// NewLayerUploadManager returns a new LayerUploadManager.
func NewLayerUploadManager(concurrencyLimit int) *LayerUploadManager {
	return &LayerUploadManager{
		tm: NewTransferManager(concurrencyLimit),
	}
}

type uploadTransfer struct {
	Transfer

	diffID layer.DiffID
	digest digest.Digest
	err    error
}

// An UploadDescriptor references a layer that may need to be uploaded.
type UploadDescriptor interface {
	// Key returns the key used to deduplicate downloads.
	Key() string
	// ID returns the ID for display purposes.
	ID() string
	// DiffID should return the DiffID for this layer.
	DiffID() layer.DiffID
	// Upload is called to perform the Upload.
	Upload(ctx context.Context, progressChan chan<- Progress) (digest.Digest, error)
}

// Upload is a blocking function which ensures the listed layers are present on
// the remote registry. It uses the string returned by the Key method to
// deduplicate uploads.
func (lum *LayerUploadManager) Upload(ctx context.Context, layers []UploadDescriptor, progressChan chan<- Progress) (map[layer.DiffID]digest.Digest, error) {
	var (
		uploads          []*uploadTransfer
		digests          = make(map[layer.DiffID]digest.Digest)
		dedupDescriptors = make(map[string]struct{})
	)

	for _, descriptor := range layers {
		progressChan <- uploadMessage(descriptor, "Preparing")

		key := descriptor.Key()
		if _, present := dedupDescriptors[key]; present {
			continue
		}
		dedupDescriptors[key] = struct{}{}

		xferFunc := lum.makeUploadFunc(descriptor)
		upload, watcher := lum.tm.Transfer(descriptor.Key(), xferFunc, progressChan, nil)
		defer upload.Release(watcher)
		uploads = append(uploads, upload.(*uploadTransfer))
	}

	for _, upload := range uploads {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-upload.Transfer.Done():
			if upload.err != nil {
				return nil, upload.err
			}
			digests[upload.diffID] = upload.digest
		}
	}

	return digests, nil
}

func (lum *LayerUploadManager) makeUploadFunc(descriptor UploadDescriptor) DoFunc {
	return func(progressChan chan<- Progress, start <-chan struct{}, inactive chan<- struct{}) Transfer {
		u := &uploadTransfer{
			Transfer: NewTransfer(),
			diffID:   descriptor.DiffID(),
		}

		go func() {
			defer func() {
				close(progressChan)
			}()

			select {
			case <-start:
			default:
				progressChan <- uploadMessage(descriptor, "Waiting")
				<-start
			}

			retries := 0
			for {
				digest, err := descriptor.Upload(u.Transfer.Context(), progressChan)
				if err == nil {
					u.digest = digest
					break
				}

				// If an error was returned because the context
				// was cancelled, we shouldn't retry.
				select {
				case <-u.Transfer.Context().Done():
					u.err = err
					return
				default:
				}

				retries++
				if _, isDNR := err.(DoNotRetry); isDNR || retries == maxUploadAttempts {
					u.err = err
					return
				}
				delay := retries * 5
				ticker := time.NewTicker(time.Second)
				for {
					progressChan <- uploadMessage(descriptor, fmt.Sprintf("Retrying in %d seconds", delay))
					delay--
					if delay == 0 {
						ticker.Stop()
						break
					}
					<-ticker.C
				}
			}
		}()

		return u
	}
}

func uploadMessage(descriptor UploadDescriptor, message string) Progress {
	return Progress{ID: descriptor.ID(), Action: message}
}
