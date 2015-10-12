package layer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/daemon/graphdriver/vfs"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/stringid"
)

func init() {
	graphdriver.ApplyUncompressedLayer = archive.UnpackLayer
	vfs.CopyWithTar = archive.CopyWithTar
}

func newTestGraphDriver(t *testing.T) (graphdriver.Driver, func()) {
	td, err := ioutil.TempDir("", "graph-")
	if err != nil {
		t.Fatal(err)
	}

	driver, err := graphdriver.GetDriver("vfs", td, nil)
	if err != nil {
		t.Fatal(err)
	}

	return driver, func() {
		os.RemoveAll(td)
	}
}

func newTestStore(t *testing.T) (Store, func()) {
	td, err := ioutil.TempDir("", "layerstore-")
	if err != nil {
		t.Fatal(err)
	}

	graph, graphcleanup := newTestGraphDriver(t)
	ls, err := NewStore(NewFileMetadataStore(td), graph)
	if err != nil {
		t.Fatal(err)
	}

	return ls, func() {
		graphcleanup()
		os.RemoveAll(td)
	}
}

type layerInit func(root string) error

func createLayer(ls Store, parent ID, layerFunc layerInit) (Layer, error) {
	containerID := stringid.GenerateRandomID()
	mount, err := ls.Mount(containerID, parent, "", nil)
	if err != nil {
		return nil, err
	}

	path, err := mount.Path()
	if err != nil {
		return nil, err
	}

	if err := layerFunc(path); err != nil {
		return nil, err
	}

	ts, err := mount.TarStream()
	if err != nil {
		return nil, err
	}

	layer, err := ls.Register(ts, parent)
	if err != nil {
		return nil, err
	}

	if err := ls.Unmount(containerID); err != nil {
		return nil, err
	}

	if _, err := ls.DeleteMount(containerID); err != nil {
		return nil, err
	}

	return layer, nil
}

type FileApplier interface {
	ApplyFile(root string) error
}

type testFile struct {
	name       string
	content    []byte
	permission os.FileMode
}

func newTestFile(name string, content []byte, perm os.FileMode) FileApplier {
	return &testFile{
		name:       name,
		content:    content,
		permission: perm,
	}
}

func (tf *testFile) ApplyFile(root string) error {
	fullPath := filepath.Join(root, tf.name)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}
	if err := ioutil.WriteFile(fullPath, tf.content, tf.permission); err != nil {
		return err
	}
	return nil
}

func initWithFiles(files ...FileApplier) layerInit {
	return func(root string) error {
		for _, f := range files {
			if err := f.ApplyFile(root); err != nil {
				return err
			}
		}
		return nil
	}
}

func releaseAndCheckDeleted(t *testing.T, ls Store, layer Layer, removed ...Layer) {
	layerCount := len(ls.(*layerStore).layerMap)
	expectedMetadata := make([]Metadata, len(removed))
	for i := range removed {
		size, err := removed[i].Size()
		if err != nil {
			t.Fatal(err)
		}

		expectedMetadata[i].LayerID = removed[i].ID()
		expectedMetadata[i].DiffID = removed[i].DiffID()
		expectedMetadata[i].Size = size
	}
	metadata, err := ls.Release(layer)
	if err != nil {
		t.Fatal(err)
	}

	if len(metadata) != len(expectedMetadata) {
		t.Fatalf("Unexpected number of deletes %d, expected %d", len(metadata), len(expectedMetadata))
	}

	for i := range metadata {
		if metadata[i] != expectedMetadata[i] {
			t.Errorf("Unexpected metadata\n\tExpected: %#v\n\tActual: %#v", expectedMetadata[i], metadata[i])
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	if expected := layerCount - len(removed); len(ls.(*layerStore).layerMap) != expected {
		t.Fatalf("Unexpected number of layers %d, expected %d", len(ls.(*layerStore).layerMap), expected)
	}
	// TODO: Done
}

func assertLayerEqual(t *testing.T, l1, l2 Layer) {
	if l1.ID() != l2.ID() {
		t.Fatalf("Mismatched ID: %s vs %s", l1.ID(), l2.ID())
	}
	if l1.DiffID() != l2.DiffID() {
		t.Fatalf("Mismatched DiffID: %s vs %s", l1.DiffID(), l2.DiffID())
	}

	size1, err := l1.Size()
	if err != nil {
		t.Fatal(err)
	}

	size2, err := l2.Size()
	if err != nil {
		t.Fatal(err)
	}

	if size1 != size2 {
		t.Fatalf("Mismatched size: %d vs %d", size1, size2)
	}

	if l1.(*cacheLayer).cacheID != l2.(*cacheLayer).cacheID {
		t.Fatalf("Mismatched cache id: %s vs %s", l1.(*cacheLayer).cacheID, l2.(*cacheLayer).cacheID)
	}

	p1, err := l1.Parent()
	if err != nil {
		t.Fatal(err)
	}

	p2, err := l2.Parent()
	if err != nil {
		t.Fatal(err)
	}

	if p1.(*cacheLayer) != nil && p2.(*cacheLayer) != nil {
		assertLayerEqual(t, p1, p2)
	} else if p1.(*cacheLayer) != nil || p2.(*cacheLayer) != nil {
		t.Fatalf("Mismatched parents: %v vs %v", p1, p2)
	}
}

func TestMountAndRegister(t *testing.T) {
	ls, cleanup := newTestStore(t)
	defer cleanup()

	li := initWithFiles(newTestFile("testfile.txt", []byte("some test data"), 0644))
	layer, err := createLayer(ls, "", li)
	if err != nil {
		t.Fatal(err)
	}

	size, _ := layer.Size()
	t.Logf("Layer size: %d", size)

	mount2, err := ls.Mount("new-test-mount", layer.ID(), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	path2, err := mount2.Path()
	if err != nil {
		t.Fatal(err)
	}

	b, err := ioutil.ReadFile(filepath.Join(path2, "testfile.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if expected := "some test data"; string(b) != expected {
		t.Fatalf("Wrong file data, expected %q, got %q", expected, string(b))
	}

	if err := ls.Unmount("new-test-mount"); err != nil {
		t.Fatal(err)
	}

	if _, err := ls.DeleteMount("new-test-mount"); err != nil {
		t.Fatal(err)
	}
}

func TestLayerRelease(t *testing.T) {
	ls, cleanup := newTestStore(t)
	defer cleanup()

	layer1, err := createLayer(ls, "", initWithFiles(newTestFile("layer1.txt", []byte("layer 1 file"), 0644)))
	if err != nil {
		t.Fatal(err)
	}

	layer2, err := createLayer(ls, layer1.ID(), initWithFiles(newTestFile("layer2.txt", []byte("layer 2 file"), 0644)))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ls.Release(layer1); err != nil {
		t.Fatal(err)
	}

	layer3a, err := createLayer(ls, layer2.ID(), initWithFiles(newTestFile("layer3.txt", []byte("layer 3a file"), 0644)))
	if err != nil {
		t.Fatal(err)
	}

	layer3b, err := createLayer(ls, layer2.ID(), initWithFiles(newTestFile("layer3.txt", []byte("layer 3b file"), 0644)))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ls.Release(layer2); err != nil {
		t.Fatal(err)
	}

	t.Logf("Layer1:  %s", layer1.ID())
	t.Logf("Layer2:  %s", layer2.ID())
	t.Logf("Layer3a: %s", layer3a.ID())
	t.Logf("Layer3b: %s", layer3b.ID())

	if expected := 4; len(ls.(*layerStore).layerMap) != expected {
		t.Fatalf("Unexpected number of layers %d, expected %d", len(ls.(*layerStore).layerMap), expected)
	}

	releaseAndCheckDeleted(t, ls, layer3b, layer3b)
	releaseAndCheckDeleted(t, ls, layer3a, layer3a, layer2, layer1)
}

func TestStoreRestore(t *testing.T) {
	ls, cleanup := newTestStore(t)
	defer cleanup()

	layer1, err := createLayer(ls, "", initWithFiles(newTestFile("layer1.txt", []byte("layer 1 file"), 0644)))
	if err != nil {
		t.Fatal(err)
	}

	layer2, err := createLayer(ls, layer1.ID(), initWithFiles(newTestFile("layer2.txt", []byte("layer 2 file"), 0644)))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ls.Release(layer1); err != nil {
		t.Fatal(err)
	}

	layer3, err := createLayer(ls, layer2.ID(), initWithFiles(newTestFile("layer3.txt", []byte("layer 3 file"), 0644)))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ls.Release(layer2); err != nil {
		t.Fatal(err)
	}

	m, err := ls.Mount("some-mount_name", layer3.ID(), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	path, err := m.Path()
	if err != nil {
		t.Fatal(err)
	}

	if err := ioutil.WriteFile(filepath.Join(path, "testfile.txt"), []byte("nothing here"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := ls.Unmount("some-mount_name"); err != nil {
		t.Fatal(err)
	}

	ls2, err := NewStore(ls.(*layerStore).store, ls.(*layerStore).driver)
	if err != nil {
		t.Fatal(err)
	}

	layer3b, err := ls2.Get(layer3.ID())
	if err != nil {
		t.Fatal(err)
	}

	assertLayerEqual(t, layer3b, layer3)

	// Mount again with same name, should already be loaded
	m2, err := ls.Mount("some-mount_name", layer3b.ID(), "", nil)
	if err != nil {
		t.Fatal(err)
	}

	path2, err := m2.Path()
	if err != nil {
		t.Fatal(err)
	}

	b, err := ioutil.ReadFile(filepath.Join(path2, "testfile.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if expected := "nothing here"; string(b) != expected {
		t.Fatalf("Unexpected content %q, expected %q", string(b), expected)
	}

	if err := ls.Unmount("some-mount_name"); err != nil {
		t.Fatal(err)
	}

	if metadata, err := ls2.DeleteMount("some-mount_name"); err != nil {
		t.Fatal(err)
	} else if len(metadata) != 0 {
		t.Fatalf("Unexpectedly deleted layers: %#v", metadata)
	}

	releaseAndCheckDeleted(t, ls2, layer3b, layer3, layer2, layer1)
}
