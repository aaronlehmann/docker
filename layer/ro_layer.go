package layer

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/Sirupsen/logrus"
)

type roLayer struct {
	chainID    ChainID
	diffID     DiffID
	parent     *roLayer
	cacheID    string
	size       int64
	layerStore *layerStore

	referenceCount int
	references     map[Layer]struct{}
}

func (rl *roLayer) TarStream() (io.ReadCloser, error) {
	r, err := rl.layerStore.store.TarSplitReader(rl.chainID)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := rl.layerStore.assembleTarTo(rl.cacheID, r, nil, pw)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				// The tarsplit file was corrupted, probably
				// because of a missing call to Close on the
				// gzip.Writer during migration in 1.10 -
				// 1.10.1. Try to repair this file by
				// recreating it. The hash isn't guaranteed to
				// match, but there's a good chance it will.
				// If it does match, we can replace the
				// tarsplit file with the new contents, and the
				// next attempt to read the layer as a tar will
				// succeed.
				rl.attemptTarSplitReconstruction()
			}

			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()
	return pr, nil
}

// attemptTarSplitReconstruction tries to fix a possibly corrupt tarsplit
// file by regenerating it. This is not guaranteed to work, because the
// hash form the new diff could end up being different than the existing DiffID.
func (rl *roLayer) attemptTarSplitReconstruction() {
	// Can only do this recovery with file-based
	// stores.
	if fileStore, ok := rl.layerStore.store.(*fileMetadataStore); ok {
		logrus.Errorf("encountered unexpected EOF error in TarStream. attempting tarsplit reconstruction.")
		parentCacheID := ""
		if rl.parent != nil {
			parentCacheID = rl.parent.cacheID
		}

		dirName, err := ioutil.TempDir(fileStore.root, "tarsplit-tmp-")
		if err != nil {
			logrus.Errorf("could not create temporary directory for tarsplit reconstruction: %v", err)
			return
		}
		defer os.RemoveAll(dirName)

		newTarSplitPath := filepath.Join(dirName, "tar-split.json.gz")
		resultDiffID, _, err := rl.layerStore.checksumForGraphIDNoTarsplit(rl.cacheID, parentCacheID, newTarSplitPath)
		if err != nil {
			logrus.Errorf("tarsplit reconstruction failed: %v", err)
			return
		}

		if resultDiffID != rl.diffID {
			logrus.Errorf("tarsplit reconstruction failed: %s != %s", resultDiffID, rl.diffID)
			return
		}

		if err = fileStore.replaceTarSplit(rl.chainID, newTarSplitPath); err != nil {
			logrus.Errorf("failed to replace tarsplit file: %v", err)
			return
		}
	}
}

func (rl *roLayer) ChainID() ChainID {
	return rl.chainID
}

func (rl *roLayer) DiffID() DiffID {
	return rl.diffID
}

func (rl *roLayer) Parent() Layer {
	if rl.parent == nil {
		return nil
	}
	return rl.parent
}

func (rl *roLayer) Size() (size int64, err error) {
	if rl.parent != nil {
		size, err = rl.parent.Size()
		if err != nil {
			return
		}
	}

	return size + rl.size, nil
}

func (rl *roLayer) DiffSize() (size int64, err error) {
	return rl.size, nil
}

func (rl *roLayer) Metadata() (map[string]string, error) {
	return rl.layerStore.driver.GetMetadata(rl.cacheID)
}

type referencedCacheLayer struct {
	*roLayer
}

func (rl *roLayer) getReference() Layer {
	ref := &referencedCacheLayer{
		roLayer: rl,
	}
	rl.references[ref] = struct{}{}

	return ref
}

func (rl *roLayer) hasReference(ref Layer) bool {
	_, ok := rl.references[ref]
	return ok
}

func (rl *roLayer) hasReferences() bool {
	return len(rl.references) > 0
}

func (rl *roLayer) deleteReference(ref Layer) {
	delete(rl.references, ref)
}

func (rl *roLayer) depth() int {
	if rl.parent == nil {
		return 1
	}
	return rl.parent.depth() + 1
}

func storeLayer(tx MetadataTransaction, layer *roLayer) error {
	if err := tx.SetDiffID(layer.diffID); err != nil {
		return err
	}
	if err := tx.SetSize(layer.size); err != nil {
		return err
	}
	if err := tx.SetCacheID(layer.cacheID); err != nil {
		return err
	}
	if layer.parent != nil {
		if err := tx.SetParent(layer.parent.chainID); err != nil {
			return err
		}
	}

	return nil
}
