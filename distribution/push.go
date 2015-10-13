package distribution

import (
	"fmt"
	"io"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/cliconfig"
	"github.com/docker/docker/daemon/events"
	"github.com/docker/docker/distribution/metadata"
	"github.com/docker/docker/images"
	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/docker/docker/registry"
	"github.com/docker/docker/tag"
	"github.com/docker/libtrust"
)

// ImagePushConfig stores push configuration.
type ImagePushConfig struct {
	// MetaHeaders store HTTP headers with metadata about the image
	// (DockerHeaders with prefix X-Meta- in the request).
	MetaHeaders map[string][]string
	// AuthConfig holds authentication credentials for authenticating with
	// the registry.
	AuthConfig *cliconfig.AuthConfig
	// Reference is the specific variant of the image to be pushed.
	// If no tag is provided, all tags will be pushed.
	Reference reference.Reference
	// OutStream is the output writer for showing the status of the push
	// operation.
	OutStream io.Writer
	// RegistryService is the registry service to use for TLS configuration
	// and endpoint lookup.
	RegistryService *registry.Service
	// EventsService is the events service to use for logging.
	EventsService *events.Events
	// LayerStore manges layers.
	LayerStore layer.Store
	// ImageStore manages images.
	ImageStore images.Store
	// TagStore manages tags.
	TagStore tag.Store
	// Pool manages concurrent pulls and pushes.
	Pool *Pool
	// TrustKey is the private key for legacy signatures. This is typically
	// an ephemeral key, since these signatures are no longer verified.
	TrustKey libtrust.PrivateKey
}

// Pusher is an interface that abstracts pushing for different API versions.
type Pusher interface {
	// Push tries to push the image configured at the creation of Pusher.
	// Push returns an error if any, as well as a boolean that determines whether to retry Push on the next configured endpoint.
	//
	// TODO(tiborvass): have Push() take a reference to repository + tag, so that the pusher itself is repository-agnostic.
	Push() (fallback bool, err error)
}

// FIXME
// Repository maps tags to image IDs.
type Repository map[string]string

// NewPusher creates a new Pusher interface that will push to either a v1 or v2
// registry. The endpoint argument contains a Version field that determines
// whether a v1 or v2 pusher will be created. The other parameters are passed
// through to the underlying pusher implementation for use during the actual
// push operation.
func NewPusher(endpoint registry.APIEndpoint, metadataStore metadata.Store, repoInfo *registry.RepositoryInfo, imagePushConfig *ImagePushConfig, sf *streamformatter.StreamFormatter) (Pusher, error) {
	switch endpoint.Version {
	case registry.APIVersion2:
		return &v2Pusher{
			blobSumLookupService:  metadata.NewBlobSumLookupService(metadataStore),
			blobSumStorageService: metadata.NewBlobSumStorageService(metadataStore),
			endpoint:              endpoint,
			repoInfo:              repoInfo,
			config:                imagePushConfig,
			sf:                    sf,
			layersPushed:          make(map[digest.Digest]bool),
		}, nil
	case registry.APIVersion1:
		// FIXME
		panic("v1 push not supported")
		/*
			return &v1Pusher{
				v1IDService: metadata.NewV1IDService(metadataStore),
				endpoint:    endpoint,
				repoInfo:    repoInfo,
				config:      imagePushConfig,
				sf:          sf,
			}, nil*/
	}
	return nil, fmt.Errorf("unknown version %d for registry %s", endpoint.Version, endpoint.URL)
}

// Push initiates a push operation on the repository named localName.
func Push(localName string, metadataDir string, imagePushConfig *ImagePushConfig) error {
	// FIXME: Allow to interrupt current push when new push of same image is done.

	var sf = streamformatter.NewJSONStreamFormatter()

	// Resolve the Repository name from fqn to RepositoryInfo
	repoInfo, err := imagePushConfig.RegistryService.ResolveRepository(localName)
	if err != nil {
		return err
	}

	endpoints, err := imagePushConfig.RegistryService.LookupPushEndpoints(repoInfo.CanonicalName)
	if err != nil {
		return err
	}

	imagePushConfig.OutStream.Write(sf.FormatStatus("", "The push refers to a repository [%s]", repoInfo.CanonicalName))

	tags := imagePushConfig.TagStore.GetRepoTags(repoInfo.LocalName)
	if len(tags) == 0 {
		return fmt.Errorf("Repository does not exist: %s", repoInfo.LocalName)
	}

	var lastErr error
	for _, endpoint := range endpoints {
		logrus.Debugf("Trying to push %s to %s %s", repoInfo.CanonicalName, endpoint.URL, endpoint.Version)

		pusher, err := NewPusher(endpoint, metadata.NewFSMetadataStore(metadataDir), repoInfo, imagePushConfig, sf)
		if err != nil {
			lastErr = err
			continue
		}
		if fallback, err := pusher.Push(); err != nil {
			if fallback {
				lastErr = err
				continue
			}
			logrus.Debugf("Not continuing with error: %v", err)
			return err

		}

		imagePushConfig.EventsService.Log("push", repoInfo.LocalName, "")
		return nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no endpoints found for %s", repoInfo.CanonicalName)
	}
	return lastErr
}
