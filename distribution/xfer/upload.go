package xfer

import (
	"github.com/docker/distribution/digest"
	"github.com/docker/docker/layer"
	"golang.org/x/net/context"
)

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

// Upload is an interface returned by the upload manager to allow getting
// the result of the nonblocking operation.
type Upload interface {
	Result() (map[layer.DiffID]digest.Digest, error)
}

type returnedUpload struct {
	digests map[layer.DiffID]digest.Digest
	err     error
}

// Result returns the top layer for the image, if all layers were obtained
// successfully.
func (d *returnedUpload) Result() (map[layer.DiffID]digest.Digest, error) {
	return d.digests, d.err
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

// Upload is a non-blocking function which ensures the listed layers
// are present on the remote registry. It uses the string returned by the Key
// method to deduplicate uploads. Once the Progress channel is closed, the
// caller may call the Result method of the Upload object.
func (lum *LayerUploadManager) Upload(ctx context.Context, layers []UploadDescriptor) (Upload, <-chan Progress) {
	// Include a buffer so that slow client connections don't affect
	// transfer performance.
	progressChan := make(chan Progress, 100)
	returnedUpload := &returnedUpload{
		digests: make(map[layer.DiffID]digest.Digest),
	}

	go func() {
		defer func() {
			close(progressChan)
		}()

		var uploads []*uploadTransfer
		dedupDescriptors := make(map[string]struct{})

		for _, descriptor := range layers {
			progressChan <- uploadMessage(descriptor, "Preparing")

			key := descriptor.Key()
			if _, present := dedupDescriptors[key]; present {
				continue
			}
			dedupDescriptors[key] = struct{}{}

			xferFunc := lum.makeUploadFunc(descriptor)
			upload := lum.tm.Transfer(descriptor.Key(), xferFunc, progressChan, nil).(*uploadTransfer)
			defer upload.Transfer.Release(progressChan)
			uploads = append(uploads, upload)
		}

		for _, upload := range uploads {
			select {
			case <-ctx.Done():
				returnedUpload.err = ctx.Err()
				return
			case <-upload.Transfer.Done():
				if upload.err != nil {
					returnedUpload.err = upload.err
					return
				}
				returnedUpload.digests[upload.diffID] = upload.digest
			}
		}
	}()

	return returnedUpload, progressChan
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

			digest, err := descriptor.Upload(u.Transfer.Context(), progressChan)
			if err != nil {
				u.err = err
				return
			}

			u.digest = digest
		}()

		return u
	}
}

func uploadMessage(descriptor UploadDescriptor, message string) Progress {
	return Progress{ID: descriptor.ID(), Action: message}
}
