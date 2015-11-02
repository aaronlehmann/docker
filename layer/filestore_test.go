package layer

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/docker/distribution/digest"
)

func randomLayerID(seed int64) ID {
	r := rand.New(rand.NewSource(seed))
	dgst, err := digest.FromBytes([]byte(fmt.Sprintf("%d", r.Int63())))
	if err != nil {
		panic(err)
	}

	return ID(dgst)
}

func newFileMetadataStore(t *testing.T) (*fileMetadataStore, string, func()) {
	td, err := ioutil.TempDir("", "layers-")
	if err != nil {
		t.Fatal(err)
	}
	fms := NewFileMetadataStore(td).(*fileMetadataStore)

	return fms, td, func() {
		if err := os.RemoveAll(td); err != nil {
			t.Logf("Failed to cleanup %q: %s", td, err)
		}
	}
}

func assertNotDirectoryError(t *testing.T, err error) {
	perr, ok := err.(*os.PathError)
	if !ok {
		t.Fatalf("Unexpected error %#v, expected path error", err)
	}

	if perr.Err != syscall.ENOTDIR {
		t.Fatalf("Unexpected error %s, expected %s", perr.Err, syscall.ENOTDIR)
	}
}

func assertPermissionError(t *testing.T, err error) {
	perr, ok := err.(*os.PathError)
	if !ok {
		t.Fatalf("Unexpected error %#v, expected path error", err)
	}

	if perr.Err != syscall.EACCES {
		t.Fatalf("Unexpected error %s, expected %s", perr.Err, syscall.EACCES)
	}
}

func TestCommitFailure(t *testing.T) {
	fms, td, cleanup := newFileMetadataStore(t)
	defer cleanup()

	if err := ioutil.WriteFile(filepath.Join(td, "sha256"), []byte("was here first!"), 0644); err != nil {
		t.Fatal(err)
	}

	tx, err := fms.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.SetSize(0); err != nil {
		t.Fatal(err)
	}

	err = tx.Commit(randomLayerID(5))
	if err == nil {
		t.Fatalf("Expected error committing with invalid layer parent directory")
	}
	assertNotDirectoryError(t, err)
}

func TestStartTransactionFailure(t *testing.T) {
	fms, td, cleanup := newFileMetadataStore(t)
	defer cleanup()

	if err := ioutil.WriteFile(filepath.Join(td, "tmp"), []byte("was here first!"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := fms.StartTransaction()
	if err == nil {
		t.Fatalf("Expected error starting transaction with invalid layer parent directory")
	}
	assertNotDirectoryError(t, err)

	if err := os.Remove(filepath.Join(td, "tmp")); err != nil {
		t.Fatal(err)
	}

	tx, err := fms.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}

	if expected := filepath.Join(td, "tmp"); strings.HasPrefix(expected, tx.String()) {
		t.Fatalf("Unexpected transaction string %q, expected prefix %q", tx.String(), expected)
	}

	if err := tx.Cancel(); err != nil {
		t.Fatal(err)
	}
}
