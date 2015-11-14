package xfer

import (
	"github.com/docker/distribution"
	"github.com/docker/distribution/digest"
	"github.com/docker/docker/layer"
	"golang.org/x/net/context"
)

type Upload interface {
	Transfer

	Result() (digest.Digest, error)
}

type LayerUploadManager struct {
	layerStore layer.Store
	tm         transferManager
}

// UploadLayer uses the transfer manager to either start a new upload, or
// attach to an in-progress upload. The blobsum of the resulting upload will be
// available through the channel returned by the BlobSum method of the returned
// object.
func (lum *LayerUploadManager) UploadLayer(ctx context.Context, repo distribution.Repository, l layer.Layer) (Upload, <-chan Progress) {
	// FIXME
	return nil, nil
}
