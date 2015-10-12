// Package layer is package for managing read only
// and read-write mounts on the union file system
// driver. Read-only mounts are refenced using a
// content hash and are protected from mutation in
// the exposed interface. The tar format is used
// to create read only layers and export both
// read only and writable layers. The exported
// tar data for a read only layer should match
// the tar used to create the layer.
package layer

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
	// ErrLayerDoesNotExist is used when an operation is
	// attempted on a layer which does not exist.
	ErrLayerDoesNotExist = errors.New("layer does not exist")
)

// ID is the content-addressable ID of a layer.
type ID digest.Digest

// DiffID is the hash of an individual layer tar.
type DiffID digest.Digest

// TarStreamer represents an object which may
// have its contents exported as a tar stream.
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

// Metadata holds information about a
// read only layer
type Metadata struct {
	// LayerID is the content hash of the layer
	LayerID ID

	// DiffID is the hash of the tar data used to
	// create the layer
	DiffID DiffID

	// Size is the size of the layer content
	Size int64
}

// MountInit is a function to initialize a
// writable mount. Changes made here will
// not be included in the Tar stream of the
// RWLayer.
type MountInit func(root string) error

// Store represents a backend for managing both
// read-only and read-write layers.
type Store interface {
	Register(io.Reader, ID) (Layer, error)
	Get(ID) (Layer, error)
	Release(Layer) ([]Metadata, error)

	Mount(id string, parent ID, label string, init MountInit) (RWLayer, error)
	Unmount(id string) error
}

// MetadataStore represents a backend for persisting
// metadata about layers and providing the metadata
// for restoring a Store.
type MetadataStore interface {
	SetSize(ID, int64) error
	SetParent(layer, parent ID) error
	SetDiffID(ID, DiffID) error
	SetCacheID(ID, string) error
	SetTarSplit(ID, io.Reader) error

	GetSize(ID) (int64, error)
	GetParent(ID) (ID, error)
	GetDiffID(ID) (DiffID, error)
	GetCacheID(ID) (string, error)
	GetTarSplit(ID) (io.ReadCloser, error)

	List() ([]ID, []string, error)
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
	store  MetadataStore
	driver graphdriver.Driver

	layerMap map[ID]*cacheLayer
	layerL   sync.Mutex

	mounts map[string]*mountedLayer
	mountL sync.Mutex
}

// NewStore creates a new Store instance using
// the provided metadata store and graph driver.
// The metadata store will be used to restore
// the Store.
func NewStore(store MetadataStore, driver graphdriver.Driver) (Store, error) {
	ls := &layerStore{
		store:    store,
		driver:   driver,
		layerMap: map[ID]*cacheLayer{},
		mounts:   map[string]*mountedLayer{},
	}

	ids, mounts, err := store.List()
	if err != nil {
		return nil, err
	}

	for _, id := range ids {
		if _, err := ls.loadLayer(id); err != nil {
			// TODO warn with bad layers, don't error out
			return nil, err
		}
	}

	// TODO: Load mounts
	logrus.Debugf("Existing mounts: %#v", mounts)

	return ls, nil
}

func (ls *layerStore) loadLayer(layer ID) (*cacheLayer, error) {
	cl, ok := ls.layerMap[layer]
	if ok {
		return cl, nil
	}

	diff, err := ls.store.GetDiffID(layer)
	if err != nil {
		return nil, err
	}

	size, err := ls.store.GetSize(layer)
	if err != nil {
		return nil, err
	}

	cacheID, err := ls.store.GetCacheID(layer)
	if err != nil {
		return nil, err
	}

	parent, err := ls.store.GetParent(layer)
	if err != nil {
		return nil, err
	}

	cl = &cacheLayer{
		address: layer,
		digest:  diff,
		size:    size,
		cacheID: cacheID,
	}

	if parent != "" {
		p, err := ls.loadLayer(parent)
		if err != nil {
			return nil, err
		}
		cl.parent = p
	}
	var pid string
	if cl.parent != nil {
		pid = cl.parent.cacheID
	}

	cl.tarStreamer = func() (io.Reader, error) {
		archiver, err := ls.driver.Diff(cl.cacheID, pid)
		return io.Reader(archiver), err
	}

	ls.layerMap[cl.address] = cl

	return cl, nil
}

func (ls *layerStore) Register(ts io.Reader, parent ID) (Layer, error) {
	var err error
	var pid string
	var p *cacheLayer
	if string(parent) != "" {
		p = ls.get(parent)
		if p == nil {
			return nil, ErrLayerDoesNotExist
		}
		pid = p.cacheID
		// Release parent chain if error
		defer func() {
			if err != nil {
				ls.layerL.Lock()
				ls.releaseLayer(p)
				ls.layerL.Unlock()
			}
		}()
	}

	// Create new cacheLayer
	layer := &cacheLayer{
		parent:         p,
		cacheID:        stringid.GenerateRandomID(),
		referenceCount: 1,
	}

	if err = ls.driver.Create(layer.cacheID, pid); err != nil {
		return nil, err
	}

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
		layer.address, err = CreateID(layer.parent.address, layer.digest)
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

	if err = ls.storeLayer(layer); err != nil {
		return nil, err
	}
	// TODO: Set tarsplit data

	ls.layerMap[layer.address] = layer

	return layer, nil
}

func (ls *layerStore) storeLayer(layer *cacheLayer) error {
	if err := ls.store.SetDiffID(layer.address, layer.digest); err != nil {
		return err
	}
	if err := ls.store.SetSize(layer.address, layer.size); err != nil {
		return err
	}
	if err := ls.store.SetCacheID(layer.address, layer.cacheID); err != nil {
		return err
	}
	if layer.parent != nil {
		if err := ls.store.SetParent(layer.address, layer.parent.address); err != nil {
			return err
		}
	}

	return nil
}

func (ls *layerStore) get(l ID) *cacheLayer {
	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	layer, ok := ls.layerMap[l]
	if !ok {
		return nil
	}

	ls.retainLayer(layer)

	return layer
}

func (ls *layerStore) Get(l ID) (Layer, error) {
	layer := ls.get(l)
	if layer == nil {
		return nil, ErrLayerDoesNotExist
	}

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

func (ls *layerStore) releaseLayer(layer *cacheLayer) {
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

	ls.releaseLayer(layer)

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

func (ls *layerStore) MountByGraphID(id string, graphID string, parent ID) (RWLayer, error) {
	// mount by already known driver ID
	// keep the mount in the array so it can be reused
	// FIXME
	ls.mounts[id] = &mountedLayer{}

	return nil, nil
}

func (ls *layerStore) Unmount(id string) error {
	ls.mountL.Lock()
	defer ls.mountL.Unlock()

	m := ls.mounts[id]
	if m == nil {
		return errors.New("mount does not exist")
	}

	// FIXME: unmount should not delete the reference, needed for remount
	delete(ls.mounts, id)

	// TODO: Issue cleanup to remove mount layer and any unretained ancestors

	return ls.driver.Put(m.mountID)
}

func (ls *layerStore) RegisterByGraphID(graphID string, parent ID, tarDataFile string) (Layer, error) {
	var err error
	var p *cacheLayer
	if string(parent) != "" {
		p = ls.get(parent)
		if p == nil {
			return nil, ErrLayerDoesNotExist
		}

		// Release parent chain if error
		defer func() {
			if err != nil {
				ls.layerL.Lock()
				ls.releaseLayer(p)
				ls.layerL.Unlock()
			}
		}()
	}

	// Create new cacheLayer
	layer := &cacheLayer{
		parent:         p,
		cacheID:        graphID,
		referenceCount: 1,
	}

	tar, err := ls.assembleTar(graphID, tarDataFile)
	if err != nil {
		return nil, err
	}

	digester := digest.Canonical.New()
	_, err = io.Copy(digester.Hash(), tar)
	if err != nil {
		return nil, err
	}
	layer.digest = DiffID(digester.Digest())

	layer.address, err = CreateID(parent, layer.digest)
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

	if err = ls.storeLayer(layer); err != nil {
		return nil, err
	}

	ls.layerMap[layer.address] = layer

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

// CreateID returns ID for a layerDigest slice and optional parent ID
func CreateID(parent ID, dgsts ...DiffID) (ID, error) {
	if len(dgsts) == 0 {
		return parent, nil
	}
	if parent == "" {
		return CreateID(ID(dgsts[0]), dgsts[1:]...)
	}
	// H = "H(n-1) SHA256(n)"
	dgst, err := digest.FromBytes([]byte(string(parent) + " " + string(dgsts[0])))
	if err != nil {
		return "", err
	}
	return CreateID(ID(dgst), dgsts[1:]...)
}
