package tarexport

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/images"
	"github.com/docker/docker/layer"
	"github.com/docker/docker/migrate/v1"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/tag"
)

type imageDescriptor struct {
	refs   []reference.NamedTagged
	layers []string
}

type saveSession struct {
	*tarexporter
	outDir      string
	images      map[images.ID]*imageDescriptor
	savedLayers map[string]struct{}
}

func (l *tarexporter) Save(names []string, outStream io.Writer) error {
	images, err := l.parseNames(names)
	if err != nil {
		return err
	}

	return (&saveSession{tarexporter: l, images: images}).save(outStream)
}

func (l *tarexporter) parseNames(names []string) (map[images.ID]*imageDescriptor, error) {
	imgDescr := make(map[images.ID]*imageDescriptor)
	for _, name := range names {
		ref, err := reference.ParseNamed(name)
		// FIXME: normalize
		if err != nil {
			return nil, err
		}

		var imgID images.ID
		if imgID, err = l.ts.Get(ref); err != nil {
			imgID, err = l.is.Search(name)
			if err != nil {
				return nil, err
			}
			ref = nil
		}

		if _, ok := imgDescr[imgID]; !ok {
			imgDescr[imgID] = &imageDescriptor{}
		}

		if ref != nil {
			var tagged reference.NamedTagged
			if _, ok := ref.(reference.Digested); ok {
				continue
			}
			var ok bool
			if tagged, ok = ref.(reference.NamedTagged); !ok {
				if tagged, err = reference.WithTag(ref, tag.DefaultTag); err != nil {
					return nil, err
				}
			}

			for _, t := range imgDescr[imgID].refs {
				if tagged.String() == t.String() {
					continue
				}
			}
			imgDescr[imgID].refs = append(imgDescr[imgID].refs, tagged)
		}

	}
	return imgDescr, nil
}

func (s *saveSession) save(outStream io.Writer) error {
	s.savedLayers = make(map[string]struct{})

	// get image json
	tempDir, err := ioutil.TempDir("", "docker-export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	s.outDir = tempDir
	reposLegacy := make(map[string]map[string]string)

	var manifest []manifestItem

	for id, imageDescr := range s.images {
		if err = s.saveImage(id); err != nil {
			return err
		}

		var repoTags []string
		var layers []string

		for _, ref := range imageDescr.refs {
			if _, ok := reposLegacy[ref.Name()]; !ok {
				reposLegacy[ref.Name()] = make(map[string]string)
			}
			reposLegacy[ref.Name()][ref.Tag()] = imageDescr.layers[len(imageDescr.layers)-1]
			repoTags = append(repoTags, ref.String())
		}

		for _, l := range imageDescr.layers {
			layers = append(layers, filepath.Join(l, legacyLayerFileName))
		}

		manifest = append(manifest, manifestItem{
			Config:   digest.Digest(id).Hex() + ".json",
			RepoTags: repoTags,
			Layers:   layers,
		})
	}

	if len(reposLegacy) > 0 {
		reposFile := filepath.Join(tempDir, legacyRepositoriesFileName)
		f, err := os.OpenFile(reposFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			f.Close()
			return err
		}
		if err := json.NewEncoder(f).Encode(reposLegacy); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := os.Chtimes(reposFile, time.Unix(0, 0), time.Unix(0, 0)); err != nil {
			return err
		}
	}

	manifestFileName := filepath.Join(tempDir, manifestFileName)
	f, err := os.OpenFile(manifestFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		f.Close()
		return err
	}
	if err := json.NewEncoder(f).Encode(manifest); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chtimes(manifestFileName, time.Unix(0, 0), time.Unix(0, 0)); err != nil {
		return err
	}

	fs, err := archive.Tar(tempDir, archive.Uncompressed)
	if err != nil {
		return err
	}
	defer fs.Close()

	if _, err := io.Copy(outStream, fs); err != nil {
		return err
	}
	return nil
}

func (s *saveSession) saveImage(id images.ID) error {
	img, err := s.is.Get(id)
	if err != nil {
		return err
	}

	diffIDs := img.RootFS.DiffIDs
	if len(diffIDs) == 0 {
		return fmt.Errorf("empty export - not implemented")
	}

	var parent digest.Digest
	var layers []string
	for i := range diffIDs {
		v1Img := images.ImageV1{}
		if i == len(diffIDs)-1 {
			v1Img = img.ImageV1
		}
		layerID, err := layer.CreateID("", diffIDs[:i+1]...)
		if err != nil {
			return err
		}
		v1ID, err := v1.CreateV1ID(v1Img, layerID, parent)
		if err != nil {
			return err
		}

		v1Img.ID = v1ID.Hex()
		if parent != "" {
			v1Img.Parent = parent.Hex()
		}

		if err := s.saveLayer(layerID, v1Img); err != nil {
			return err
		}
		layers = append(layers, v1Img.ID)
		parent = v1ID
	}

	configFile := filepath.Join(s.outDir, digest.Digest(id).Hex()+".json")
	if err := ioutil.WriteFile(configFile, img.RawJSON(), 0644); err != nil {
		return err
	}
	if err := os.Chtimes(configFile, time.Unix(0, 0), time.Unix(0, 0)); err != nil {
		return err
	}

	s.images[id].layers = layers
	return nil
}

func (s *saveSession) saveLayer(id layer.ID, legacyImg images.ImageV1) error {
	if _, exists := s.savedLayers[legacyImg.ID]; exists {
		return nil
	}

	outDir := filepath.Join(s.outDir, legacyImg.ID)
	if err := os.Mkdir(outDir, 0755); err != nil {
		return err
	}

	// todo: why is this version file here?
	if err := ioutil.WriteFile(filepath.Join(outDir, legacyVersionFileName), []byte("1.0"), 0644); err != nil {
		return err
	}

	imageConfig, err := json.Marshal(legacyImg)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(filepath.Join(outDir, legacyConfigFileName), imageConfig, 0644); err != nil {
		return err
	}

	// serialize filesystem
	tarFile, err := os.Create(filepath.Join(outDir, legacyLayerFileName))
	if err != nil {
		return err
	}
	defer tarFile.Close()

	l, err := s.ls.Get(id)
	if err != nil {
		return err
	}
	defer s.ls.Release(l)

	arch, err := l.TarStream()
	if err != nil {
		return err
	}
	if _, err := io.Copy(tarFile, arch); err != nil {
		return err
	}

	for _, fname := range []string{"", legacyVersionFileName, legacyConfigFileName, legacyLayerFileName} {
		// todo: maybe save layer created timestamp?
		if err := os.Chtimes(filepath.Join(outDir, fname), time.Unix(0, 0), time.Unix(0, 0)); err != nil {
			return err
		}
	}

	s.savedLayers[legacyImg.ID] = struct{}{}
	return nil
}
