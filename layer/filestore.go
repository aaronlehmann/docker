package layer

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/digest"
	"github.com/docker/docker/pkg/ioutils"
)

var stringIDRegexp = regexp.MustCompile(`^[a-f0-9]{64}(-init)?$`) // FIXME: probably better to keep the old format

type fileMetadataStore struct {
	root string
}

type fileMetadataTransaction struct {
	store *fileMetadataStore
	root  string
}

// NewFileMetadataStore returns an instance of a metadata store
// which is backed by files on disk using the provided root
// as the root of metadata files.
func NewFileMetadataStore(root string) MetadataStore {
	return &fileMetadataStore{
		root: root,
	}
}

func (fms *fileMetadataStore) getLayerDirectory(layer ID) string {
	dgst := digest.Digest(layer)
	return filepath.Join(fms.root, string(dgst.Algorithm()), dgst.Hex())
}

func (fms *fileMetadataStore) getLayerFilename(layer ID, filename string) string {
	return filepath.Join(fms.getLayerDirectory(layer), filename)
}

func (fms *fileMetadataStore) getMountDirectory(mount string) string {
	return filepath.Join(fms.root, "mounts", mount)
}

func (fms *fileMetadataStore) getMountFilename(mount, filename string) string {
	return filepath.Join(fms.getMountDirectory(mount), filename)
}

func (fms *fileMetadataStore) StartTransaction() (MetadataTransaction, error) {
	tmpDir := filepath.Join(fms.root, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, err
	}

	td, err := ioutil.TempDir(tmpDir, "layer-")
	if err != nil {
		return nil, err
	}
	// Create a new tempdir
	return &fileMetadataTransaction{
		store: fms,
		root:  td,
	}, nil
}

func (fm *fileMetadataTransaction) SetSize(size int64) error {
	content := fmt.Sprintf("%d", size)
	return ioutil.WriteFile(filepath.Join(fm.root, "size"), []byte(content), 0644)
}

func (fm *fileMetadataTransaction) SetParent(parent ID) error {
	return ioutil.WriteFile(filepath.Join(fm.root, "parent"), []byte(digest.Digest(parent).String()), 0644)
}

func (fm *fileMetadataTransaction) SetDiffID(diff DiffID) error {
	return ioutil.WriteFile(filepath.Join(fm.root, "diff"), []byte(digest.Digest(diff).String()), 0644)
}

func (fm *fileMetadataTransaction) SetCacheID(cacheID string) error {
	return ioutil.WriteFile(filepath.Join(fm.root, "cache-id"), []byte(cacheID), 0644)
}

func (fm *fileMetadataTransaction) TarSplitWriter() (io.WriteCloser, error) {
	f, err := os.OpenFile(filepath.Join(fm.root, "tar-split.json.gz"), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	fz := gzip.NewWriter(f)

	return ioutils.NewWriteCloserWrapper(fz, func() error {
		fz.Close()
		return f.Close()
	}), nil
}

func (fm *fileMetadataTransaction) Commit(layer ID) error {
	finalDir := fm.store.getLayerDirectory(layer)
	if err := os.MkdirAll(filepath.Dir(finalDir), 0755); err != nil {
		return err
	}
	return os.Rename(fm.root, finalDir)
}

func (fm *fileMetadataTransaction) Cancel() error {
	return os.RemoveAll(fm.root)
}

func (fm *fileMetadataTransaction) String() string {
	return fm.root
}

func (fms *fileMetadataStore) GetSize(layer ID) (int64, error) {
	content, err := ioutil.ReadFile(fms.getLayerFilename(layer, "size"))
	if err != nil {
		return 0, err
	}

	size, err := strconv.ParseInt(string(content), 10, 64)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (fms *fileMetadataStore) GetParent(layer ID) (ID, error) {
	content, err := ioutil.ReadFile(fms.getLayerFilename(layer, "parent"))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	dgst, err := digest.ParseDigest(string(content))
	if err != nil {
		return "", err
	}

	return ID(dgst), nil
}

func (fms *fileMetadataStore) GetDiffID(layer ID) (DiffID, error) {
	content, err := ioutil.ReadFile(fms.getLayerFilename(layer, "diff"))
	if err != nil {
		return "", err
	}

	dgst, err := digest.ParseDigest(string(content))
	if err != nil {
		return "", err
	}

	return DiffID(dgst), nil
}

func (fms *fileMetadataStore) GetCacheID(layer ID) (string, error) {
	content, err := ioutil.ReadFile(fms.getLayerFilename(layer, "cache-id"))
	if err != nil {
		return "", err
	}

	if !stringIDRegexp.MatchString(string(content)) {
		return "", errors.New("invalid cache id value")
	}

	return string(content), nil
}

func (fms *fileMetadataStore) TarSplitReader(layer ID) (io.ReadCloser, error) {
	fz, err := os.Open(fms.getLayerFilename(layer, "tar-split.json.gz"))
	if err != nil {
		return nil, err
	}
	f, err := gzip.NewReader(fz)
	if err != nil {
		return nil, err
	}

	return ioutils.NewReadCloserWrapper(f, func() error {
		f.Close()
		return fz.Close()
	}), nil
}

func (fms *fileMetadataStore) SetMountID(mount string, mountID string) error {
	if err := os.MkdirAll(fms.getMountDirectory(mount), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(fms.getMountFilename(mount, "mount-id"), []byte(mountID), 0644)
}

func (fms *fileMetadataStore) SetInitID(mount string, init string) error {
	if err := os.MkdirAll(fms.getMountDirectory(mount), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(fms.getMountFilename(mount, "init-id"), []byte(init), 0644)
}

func (fms *fileMetadataStore) SetMountParent(mount string, parent ID) error {
	if err := os.MkdirAll(fms.getMountDirectory(mount), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(fms.getMountFilename(mount, "parent"), []byte(digest.Digest(parent).String()), 0644)
}

func (fms *fileMetadataStore) GetMountID(mount string) (string, error) {
	content, err := ioutil.ReadFile(fms.getMountFilename(mount, "mount-id"))
	if err != nil {
		return "", err
	}

	if !stringIDRegexp.MatchString(string(content)) {
		return "", errors.New("invalid mount id value")
	}

	return string(content), nil
}

func (fms *fileMetadataStore) GetInitID(mount string) (string, error) {
	content, err := ioutil.ReadFile(fms.getMountFilename(mount, "init-id"))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	if !stringIDRegexp.MatchString(string(content)) {
		return "", errors.New("invalid init id value")
	}

	return string(content), nil
}

func (fms *fileMetadataStore) GetMountParent(mount string) (ID, error) {
	content, err := ioutil.ReadFile(fms.getMountFilename(mount, "parent"))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	dgst, err := digest.ParseDigest(string(content))
	if err != nil {
		return "", err
	}

	return ID(dgst), nil
}

func (fms *fileMetadataStore) List() ([]ID, []string, error) {
	fileInfos, err := ioutil.ReadDir(fms.root)
	if err != nil {
		if os.IsNotExist(err) {
			return []ID{}, []string{}, nil
		}
		return nil, nil, err
	}

	var ids []ID
	for _, fi := range fileInfos {
		if fi.IsDir() && fi.Name() != "mounts" {
			dgst, err := digest.ParseDigest(fi.Name())
			if err == nil {
				ids = append(ids, ID(dgst))
			} else {
				logrus.Debugf("Ignoring invalid directory %s", fi.Name())
			}
		}
	}

	fileInfos, err = ioutil.ReadDir(filepath.Join(fms.root, "mounts"))
	if err != nil {
		if os.IsNotExist(err) {
			return ids, []string{}, nil
		}
		return nil, nil, err
	}

	var mounts []string
	for _, fi := range fileInfos {
		if fi.IsDir() {
			mounts = append(mounts, fi.Name())
		}
	}

	return ids, mounts, nil
}

func (fms *fileMetadataStore) Remove(layer ID) error {
	return os.RemoveAll(fms.getLayerDirectory(layer))
}

func (fms *fileMetadataStore) RemoveMount(mount string) error {
	return os.RemoveAll(fms.getMountDirectory(mount))
}
