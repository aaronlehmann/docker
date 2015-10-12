package layers

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/digest"
	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/stringid"

	"github.com/vbatts/tar-split/tar/asm"
	"github.com/vbatts/tar-split/tar/storage"
)

var (
	ErrLayerDoesNotExist = errors.New("layer does not exist")
)

// ID is the content-addressable ID of a layer.
type ID digest.Digest

// DiffID is the hash of an individual layer tar.
type DiffID digest.Digest

type TarStreamer interface {
	TarStream() (io.Reader, error)
}

// Layer represents a read only layer
type Layer interface {
	TarStreamer
	ID() ID
	DiffID() DiffID
	Parent() (Layer, error)
	Size() (int64, error)
}

// RWLayer represents a layer which is
// read and writable
type RWLayer interface {
	TarStreamer
	Path() (string, error)
	Parent() (Layer, error)
}

type Metadata struct {
	LayerID ID
	DiffID  DiffID
	Size    int64
}

// MountInit is a function to initialize a
// writable mount. Changes made here will
// not be included in the Tar stream of the
// RWLayer.
type MountInit func(root string) error

type LayerStore interface {
	Register(io.Reader, ID) (Layer, error)
	Get(ID) (Layer, error)
	Release(Layer) ([]Metadata, error)

	Mount(id string, parent ID, label string, init MountInit) (RWLayer, error)
	Unmount(id string) error
}

type tarStreamer func() (io.Reader, error)

type cacheLayer struct {
	tarStreamer
	address ID
	digest  DiffID
	parent  *cacheLayer
	cacheID string
	size    int64

	referenceCount int
}

func (cl *cacheLayer) TarStream() (io.Reader, error) {
	return cl.tarStreamer()
}

func (cl *cacheLayer) ID() ID {
	return cl.address
}

func (cl *cacheLayer) DiffID() DiffID {
	return cl.digest
}

func (cl *cacheLayer) Parent() (Layer, error) {
	return cl.parent, nil
}

func (cl *cacheLayer) Size() (int64, error) {
	return cl.size, nil
}

type mountedLayer struct {
	tarStreamer
	mountID string
	parent  *cacheLayer
	path    string
}

func (ml *mountedLayer) TarStream() (io.Reader, error) {
	return ml.tarStreamer()
}

func (ml *mountedLayer) Path() (string, error) {
	return ml.path, nil
}

func (ml *mountedLayer) Parent() (Layer, error) {
	return ml.parent, nil
}

type layerStore struct {
	root   string
	driver graphdriver.Driver

	layerMap map[ID]*cacheLayer
	layerL   sync.Mutex

	mounts map[string]*mountedLayer
	mountL sync.Mutex
}

func NewLayerStore(root string, driver graphdriver.Driver) (LayerStore, error) {
	ls := &layerStore{
		root:     root,
		driver:   driver,
		layerMap: map[ID]*cacheLayer{},
		mounts:   map[string]*mountedLayer{},
	}

	// TODO: Load existing layers and references

	return ls, nil
}

func (ls *layerStore) Register(ts io.Reader, parent ID) (Layer, error) {
	var pid string
	var p *cacheLayer
	if string(parent) != "" {
		l, ok := ls.layerMap[parent]
		if !ok {
			return nil, ErrLayerDoesNotExist
		}
		p = l
		pid = l.cacheID
	}

	// Create new cacheLayer
	layer := &cacheLayer{
		parent:  p,
		cacheID: stringid.GenerateRandomID(),
	}

	if err := ls.driver.Create(layer.cacheID, pid); err != nil {
		return nil, err
	}

	var err error
	defer func() {
		if err != nil {
			logrus.Debugf("Cleaning up layer %s: %v", layer.cacheID, err)
			if err := ls.driver.Remove(layer.cacheID); err != nil {
				logrus.Errorf("Error cleaning up cache layer %s: %v", layer.cacheID, err)
			}
		}
	}()

	digester := digest.Canonical.New()
	tr := io.TeeReader(ts, digester.Hash())

	layer.size, err = ls.driver.ApplyDiff(layer.cacheID, pid, archive.Reader(tr))
	if err != nil {
		return nil, err
	}

	layer.tarStreamer = func() (io.Reader, error) {
		archiver, err := ls.driver.Diff(layer.cacheID, pid)
		return io.Reader(archiver), err
	}

	layer.digest = DiffID(digester.Digest())

	if layer.parent == nil {
		layer.address = ID(layer.digest)
	} else {
		layer.address, err = LayerID(layer.parent.address, layer.digest)
		if err != nil {
			return nil, err
		}
	}

	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	if existingLayer, ok := ls.layerMap[layer.address]; ok {
		// Set error for cleanup, but do not return
		err = errors.New("layer already exists")
		return existingLayer, nil
	}

	ls.layerMap[layer.address] = layer

	ls.retainLayer(layer)

	// TODO: Persist mapping update to disk

	return layer, nil
}

func (ls *layerStore) Get(l ID) (Layer, error) {
	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	layer, ok := ls.layerMap[l]
	if !ok {
		return nil, ErrLayerDoesNotExist
	}

	ls.retainLayer(layer)

	return layer, nil
}

func (ls *layerStore) retainLayer(layer *cacheLayer) {
	for l := layer; ; l = l.parent {
		l.referenceCount++
		if l.parent == nil {
			break
		}
	}
}

func (ls *layerStore) cleanup() ([]Metadata, error) {
	// Mark
	layers := []*cacheLayer{}
	for id, layer := range ls.layerMap {
		if layer.referenceCount == 0 {
			layers = append(layers, layer)
			delete(ls.layerMap, id)
		}
	}

	// Order
	// if is parent, order after, since
	// loops are not possible due to linking
	// by content hash, this will always
	// converge and complete
	for i := 0; i < len(layers); {
		layer := layers[i]
		newPosition := i
		for j := i + 1; j < len(layers); j++ {
			if layers[j].parent == layer {
				newPosition = j
			}
		}
		if newPosition > i {
			// Shift and continue at same index
			copy(layers[i:newPosition], layers[i+1:newPosition+1])
			layers[newPosition] = layer
		} else {
			// Properly ordered, move to next
			i++
		}
	}

	// Sweep
	metadata := make([]Metadata, len(layers))
	var lastErr error
	for i, layer := range layers {
		metadata[i].DiffID = layer.digest
		metadata[i].LayerID = layer.address
		metadata[i].Size = layer.size
		if err := ls.driver.Remove(layer.cacheID); err != nil {
			// TODO: Should this continue, log and return last?
			lastErr = err
		}
		// TODO: Delete from storage
	}

	return metadata, lastErr
}

func (ls *layerStore) Release(l Layer) ([]Metadata, error) {
	ls.layerL.Lock()
	defer ls.layerL.Unlock()
	layer, ok := ls.layerMap[l.ID()]
	if !ok {
		return []Metadata{}, nil
	}

	for l := layer; ; l = l.parent {
		if l.referenceCount < 2 {
			l.referenceCount = 0
		} else {
			l.referenceCount--
		}
		if l.parent == nil {
			break
		}
	}

	return ls.cleanup()
}

func (ls *layerStore) Mount(id string, parent ID, mountLabel string, initFunc MountInit) (RWLayer, error) {
	ls.mountL.Lock()
	defer ls.mountL.Unlock()
	if m, ok := ls.mounts[id]; ok {
		return m, nil
	}

	//TODO: Call get to fully retain
	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	var pid string
	var p *cacheLayer
	if string(parent) != "" {
		l, ok := ls.layerMap[parent]
		if !ok {
			return nil, ErrLayerDoesNotExist
		}
		p = l
		pid = l.cacheID
	}

	mount := &mountedLayer{
		parent:  p,
		mountID: stringid.GenerateRandomID(),
	}

	if err := ls.driver.Create(mount.mountID, pid); err != nil {
		return nil, err
	}

	mount.tarStreamer = func() (io.Reader, error) {
		archiver, err := ls.driver.Diff(mount.mountID, pid)
		return io.Reader(archiver), err
	}

	dir, err := ls.driver.Get(mount.mountID, "")
	if err != nil {
		// TODO: Cleanup
		return nil, err
	}
	mount.path = dir

	ls.mounts[id] = mount

	// TODO: Persist mapping update to disk

	return mount, nil
}

func (ls *layerStore) Unmount(id string) error {
	ls.mountL.Lock()
	defer ls.mountL.Unlock()

	m := ls.mounts[id]
	if m == nil {
		return errors.New("mount does not exist")
	}

	delete(ls.mounts, id)

	// TODO: Issue cleanup to remove mount layer and any unretained ancestors

	return ls.driver.Put(m.mountID)
}

func (ls *layerStore) RegisterOnDisk(cacheID string, parent ID, tarDataFile string) (Layer, error) {
	var p *cacheLayer
	if string(parent) != "" {
		l, ok := ls.layerMap[parent]
		if !ok {
			return nil, ErrLayerDoesNotExist
		}
		p = l
	}

	// Create new cacheLayer
	layer := &cacheLayer{
		parent:  p,
		cacheID: cacheID,
	}

	tar, err := ls.assembleTar(cacheID, tarDataFile)
	if err != nil {
		return nil, err
	}

	digester := digest.Canonical.New()
	if _, err := io.Copy(digester.Hash(), tar); err != nil {
		return nil, err
	}
	layer.digest = DiffID(digester.Digest())

	layer.address, err = LayerID(parent, layer.digest)
	if err != nil {
		return nil, err
	}

	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	if existingLayer, ok := ls.layerMap[layer.address]; ok {
		// Set error for cleanup, but do not return
		err = errors.New("layer already exists")
		return existingLayer, nil
	}

	ls.layerMap[layer.address] = layer
	ls.retainLayer(layer)

	return layer, nil
}

func (ls *layerStore) assembleTar(cacheID, tarDataFile string) (io.Reader, error) {
	mf, err := os.Open(tarDataFile)
	if err != nil {
		if !os.IsNotExist(err) {
			// todo: recreation
		}
		return nil, err
	}
	pR, pW := io.Pipe()
	// this will need to be in a goroutine, as we are returning the stream of a
	// tar archive, but can not close the metadata reader early (when this
	// function returns)...
	go func() {
		defer mf.Close()
		// let's reassemble!
		logrus.Debugf("[graph] TarLayer with reassembly: %s", cacheID)
		mfz, err := gzip.NewReader(mf)
		if err != nil {
			pW.CloseWithError(fmt.Errorf("[graph] error with %s:  %s", tarDataFile, err))
			return
		}
		defer mfz.Close()

		// get our relative path to the container
		fsLayer, err := ls.driver.Get(cacheID, "")
		if err != nil {
			pW.CloseWithError(err)
			return
		}
		defer ls.driver.Put(cacheID)

		metaUnpacker := storage.NewJSONUnpacker(mfz)
		fileGetter := storage.NewPathFileGetter(fsLayer)
		logrus.Debugf("[graph] %s is at %q", cacheID, fsLayer)
		ots := asm.NewOutputTarStream(fileGetter, metaUnpacker)
		defer ots.Close()
		if _, err := io.Copy(pW, ots); err != nil {
			pW.CloseWithError(err)
			return
		}
		pW.Close()
	}()
	return pR, nil
}

// LayerID returns ID for a layerDigest slice and optional parent ID
func LayerID(parent ID, dgsts ...DiffID) (ID, error) {
	if len(dgsts) == 0 {
		return parent, nil
	}
	if parent == "" {
		return LayerID(ID(dgsts[0]), dgsts[1:]...)
	}
	// H = "H(n-1) SHA256(n)"
	dgst, err := digest.FromBytes([]byte(string(parent) + " " + string(dgsts[0])))
	if err != nil {
		return "", err
	}
	return LayerID(ID(dgst), dgsts[1:]...)
}
