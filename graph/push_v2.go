package graph

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution"
	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/manifest"
	"github.com/docker/docker/image"
	"github.com/docker/docker/pkg/progressreader"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/docker/docker/pkg/stringid"
	"github.com/docker/docker/registry"
	"github.com/docker/docker/runconfig"
	"github.com/docker/docker/utils"
	"golang.org/x/net/context"
)

type v2Pusher struct {
	*TagStore
	endpoint  registry.APIEndpoint
	localRepo Repository
	repoInfo  *registry.RepositoryInfo
	config    *ImagePushConfig
	sf        *streamformatter.StreamFormatter
	repo      distribution.Repository
}

func (p *v2Pusher) Push() (fallback bool, err error) {
	p.repo, err = NewV2Repository(p.repoInfo, p.endpoint, p.config.MetaHeaders, p.config.AuthConfig)
	if err != nil {
		logrus.Debugf("Error getting v2 registry: %v", err)
		return true, err
	}
	return false, p.pushV2Repository(p.config.Tag)
}

func (p *v2Pusher) getImageTags(askedTag string) ([]string, error) {
	logrus.Debugf("Checking %q against %#v", askedTag, p.localRepo)
	if len(askedTag) > 0 {
		if _, ok := p.localRepo[askedTag]; !ok || utils.DigestReference(askedTag) {
			return nil, fmt.Errorf("Tag does not exist for %s", askedTag)
		}
		return []string{askedTag}, nil
	}
	var tags []string
	for tag := range p.localRepo {
		if !utils.DigestReference(tag) {
			tags = append(tags, tag)
		}
	}
	return tags, nil
}

func (p *v2Pusher) pushV2Repository(tag string) (err error) {
	localName := p.repoInfo.LocalName
	taggedName := localName
	if len(tag) > 0 {
		taggedName = utils.ImageReference(localName, tag)
	}
	broadcaster, found := p.poolAdd("push", taggedName)
	if found {
		// Another push or pull of the same repository is already taking
		// place; just wait for it to finish
		broadcaster.Add(p.config.OutStream)
		result := broadcaster.Wait()
		if err, ok := result.(error); ok {
			return err
		}
		return nil
	}
	defer func() {
		if err != nil {
			p.poolRemove("push", taggedName, err)
		} else {
			p.poolRemove("push", taggedName, nil)
		}
	}()
	broadcaster.Add(p.config.OutStream)

	var tags []string
	tags, err = p.getImageTags(tag)
	if err != nil {
		return fmt.Errorf("error getting tags for %s: %s", localName, err)
	}
	if len(tags) == 0 {
		return fmt.Errorf("no tags to push for %s", localName)
	}

	for _, tag := range tags {
		if err = p.pushV2Tag(broadcaster, tag); err != nil {
			return err
		}
	}

	return nil
}

func (p *v2Pusher) pushV2Tag(out io.Writer, tag string) error {
	logrus.Debugf("Pushing repository: %s:%s", p.repo.Name(), tag)

	layerID, exists := p.localRepo[tag]
	if !exists {
		return fmt.Errorf("tag does not exist: %s", tag)
	}

	layersSeen := make(map[string]bool)

	layer, err := p.graph.Get(layerID)
	if err != nil {
		return err
	}

	m := &manifest.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name:         p.repo.Name(),
		Tag:          tag,
		Architecture: layer.Architecture,
		FSLayers:     []manifest.FSLayer{},
		History:      []manifest.History{},
	}

	var metadata runconfig.Config
	if layer != nil && layer.Config != nil {
		metadata = *layer.Config
	}

	for ; layer != nil; layer, err = p.graph.GetParent(layer) {
		if err != nil {
			return err
		}

		if layersSeen[layer.ID] {
			break
		}

		logrus.Debugf("Pushing layer: %s", layer.ID)

		if layer.Config != nil && metadata.Image != layer.ID {
			if err := runconfig.Merge(&metadata, layer.Config); err != nil {
				return err
			}
		}

		jsonData, err := p.graph.RawJSON(layer.ID)
		if err != nil {
			return fmt.Errorf("cannot retrieve the path for %s: %s", layer.ID, err)
		}

		var exists bool
		dgst, err := p.graph.GetDigest(layer.ID)
		switch err {
		case nil:
			_, err := p.repo.Blobs(context.Background()).Stat(context.Background(), dgst)
			switch err {
			case nil:
				exists = true
				out.Write(p.sf.FormatProgress(stringid.TruncateID(layer.ID), "Image already exists", nil))
			case distribution.ErrBlobUnknown:
				// nop
			default:
				out.Write(p.sf.FormatProgress(stringid.TruncateID(layer.ID), "Image push failed", nil))
				return err
			}
		case ErrDigestNotSet:
			// nop
		case digest.ErrDigestInvalidFormat, digest.ErrDigestUnsupported:
			return fmt.Errorf("error getting image checksum: %v", err)
		}

		// if digest was empty or not saved, or if blob does not exist on the remote repository,
		// then fetch it.
		if !exists {
			if pushDigest, err := p.pushV2Image(out, p.repo.Blobs(context.Background()), layer); err != nil {
				return err
			} else if pushDigest != dgst {
				// Cache new checksum
				if err := p.graph.SetDigest(layer.ID, pushDigest); err != nil {
					return err
				}
				dgst = pushDigest
			}
		}

		m.FSLayers = append(m.FSLayers, manifest.FSLayer{BlobSum: dgst})
		m.History = append(m.History, manifest.History{V1Compatibility: string(jsonData)})

		layersSeen[layer.ID] = true
	}

	logrus.Infof("Signed manifest for %s:%s using daemon's key: %s", p.repo.Name(), tag, p.trustKey.KeyID())
	signed, err := manifest.Sign(m, p.trustKey)
	if err != nil {
		return err
	}

	manifestDigest, manifestSize, err := digestFromManifest(signed, p.repo.Name())
	if err != nil {
		return err
	}
	if manifestDigest != "" {
		out.Write(p.sf.FormatStatus("", "%s: digest: %s size: %d", tag, manifestDigest, manifestSize))
	}

	manSvc, err := p.repo.Manifests(context.Background())
	if err != nil {
		return err
	}
	return manSvc.Put(signed)
}

func (p *v2Pusher) pushV2Image(out io.Writer, bs distribution.BlobService, img *image.Image) (dgst digest.Digest, err error) {
	// We must include the repo name in this key, because cross-repo pushes
	// may not be deduplicated.
	poolKey := "layerpush:" + p.repoInfo.LocalName + ":" + img.ID
	transferBroadcaster, found := p.poolAdd("push", poolKey)
	transferBroadcaster.Add(out)
	if found {
		// This layer is already being uploaded. Wait for the upload to
		// complete.
		result := transferBroadcaster.Wait()
		switch v := result.(type) {
		case error:
			return "", v
		case digest.Digest:
			return v, nil
		default:
			return "", errors.New("unexpected type in returned by broadcaster in pushV2Image")
		}
	}

	defer func() {
		if err != nil {
			p.poolRemove("push", poolKey, err)
		} else {
			p.poolRemove("push", poolKey, dgst)
		}
	}()

	transferBroadcaster.Write(p.sf.FormatProgress(stringid.TruncateID(img.ID), "Preparing", nil))

	image, err := p.graph.Get(img.ID)
	if err != nil {
		return "", err
	}
	arch, err := p.graph.TarLayer(image)
	if err != nil {
		return "", err
	}
	defer arch.Close()

	// Send the layer
	layerUpload, err := bs.Create(context.Background())
	if err != nil {
		return "", err
	}
	defer layerUpload.Close()

	digester := digest.Canonical.New()
	tee := io.TeeReader(arch, digester.Hash())

	reader := progressreader.New(progressreader.Config{
		In:        ioutil.NopCloser(tee), // we'll take care of close here.
		Out:       transferBroadcaster,
		Formatter: p.sf,

		// TODO(stevvooe): This may cause a size reporting error. Try to get
		// this from tar-split or elsewhere. The main issue here is that we
		// don't want to buffer to disk *just* to calculate the size.
		Size: img.Size,

		NewLines: false,
		ID:       stringid.TruncateID(img.ID),
		Action:   "Pushing",
	})

	transferBroadcaster.Write(p.sf.FormatProgress(stringid.TruncateID(img.ID), "Pushing", nil))
	nn, err := io.Copy(layerUpload, reader)
	if err != nil {
		return "", err
	}

	dgst = digester.Digest()
	if _, err := layerUpload.Commit(context.Background(), distribution.Descriptor{Digest: dgst}); err != nil {
		return "", err
	}

	logrus.Debugf("uploaded layer %s (%s), %d bytes", img.ID, dgst, nn)
	transferBroadcaster.Write(p.sf.FormatProgress(stringid.TruncateID(img.ID), "Pushed", nil))

	return dgst, nil
}
