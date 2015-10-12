package tag

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/images"
)

var (
	// ErrNoName is returned if a reference.Reference variable does not
	// implement Name.
	ErrNoName = errors.New("reference has no name")
	// ErrDoesNotExist is returned if a reference is not found in the
	// store.
	ErrDoesNotExist = errors.New("reference does not exist")
	// ErrNoTagOrDigest is returned if a reference has neither a Tag nor
	// a Digest.
	ErrNoTagOrDigest = errors.New("reference has neither a tag nor a digest")
)

// An Association is a tuple associating a reference with an image ID.
type Association struct {
	Ref     reference.Reference
	ImageID images.ID
}

// Store provides the set of methods which can operate on a tag store.
type Store interface {
	References(id images.ID) []reference.Reference
	ReferencesByName(ref reference.Named) []Association
	Add(ref reference.Reference, id images.ID, force bool) error
	Delete(ref reference.Reference) (bool, error)
	Get(ref reference.Reference) (images.ID, error)
}

type store struct {
	sync.RWMutex
	jsonPath string
	// Repositories is a map of repositories, indexed by name.
	Repositories map[string]repository
}

// Repository maps tags to image IDs. The key is a a stringified Reference,
// including the repository name.
type repository map[string]images.ID

// NewTagStore creates a new tag store, tied to a file path where the set of
// tags is serialized in JSON format.
func NewTagStore(jsonPath string) (Store, error) {
	abspath, err := filepath.Abs(jsonPath)
	if err != nil {
		return nil, err
	}

	store := &store{
		jsonPath:     abspath,
		Repositories: make(map[string]repository),
	}
	// Load the json file if it exists, otherwise create it.
	if err := store.reload(); os.IsNotExist(err) {
		if err := store.save(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return store, nil
}

// Add adds a tag or digest to the store. If force is set to true, existing
// references can be overwritten. This only works for tags, not digests.
func (store *store) Add(ref reference.Reference, id images.ID, force bool) error {
	// Reference must include a name and a tag or digest
	named, isNamed := ref.(reference.Named)
	if !isNamed {
		return ErrNoName
	}

	switch ref.(type) {
	case reference.Tagged:
	case reference.Digested:
		break
	default:
		return ErrNoTagOrDigest
	}

	store.Lock()
	defer store.Unlock()

	repository, exists := store.Repositories[named.Name()]
	if !exists || repository == nil {
		repository = make(map[string]images.ID)
		store.Repositories[named.Name()] = repository
	}

	refStr := ref.String()
	oldID, exists := repository[refStr]

	if exists {
		// force only works for tags
		if digested, isDigest := ref.(reference.Digested); isDigest {
			return fmt.Errorf("Cannot overwrite digest %s", digested.Digest().String)
		}

		if !force {
			return fmt.Errorf("Conflict: Tag %s is already set to image %s, if you want to replace it, please use -f option", ref.(reference.Tagged).Tag(), oldID.String())
		}
	}

	repository[refStr] = id

	return store.save()
}

// Delete deletes a reference from the store. It returns true if a deletion
// happened, or false otherwise.
func (store *store) Delete(ref reference.Reference) (bool, error) {
	// Reference must include a name
	named, isNamed := ref.(reference.Named)
	if !isNamed {
		return false, ErrNoName
	}

	store.Lock()
	defer store.Unlock()

	repoName := named.Name()

	repository, exists := store.Repositories[repoName]
	if !exists {
		return false, ErrDoesNotExist
	}

	switch ref.(type) {
	case reference.Tagged:
	case reference.Digested:
	default:
		// Delete the whole repository.
		delete(store.Repositories, repoName)
		return true, store.save()
	}

	refStr := ref.String()
	if _, exists := repository[refStr]; exists {
		delete(repository, refStr)
		if len(repository) == 0 {
			delete(store.Repositories, repoName)
		}
		return true, store.save()
	}

	return false, ErrDoesNotExist
}

// Get retrieves an item from the store by reference.
func (store *store) Get(ref reference.Reference) (images.ID, error) {
	named, isNamed := ref.(reference.Named)
	if !isNamed {
		return "", ErrNoName
	}

	store.RLock()
	defer store.RUnlock()

	repository, exists := store.Repositories[named.Name()]
	if !exists || repository == nil {
		return "", ErrDoesNotExist
	}

	id, exists := repository[ref.String()]
	if !exists {
		return "", ErrDoesNotExist
	}

	return id, nil
}

// References returns a slice of references to the given image ID. The slice
// will be nil if there are no references to this image ID.
func (store *store) References(id images.ID) []reference.Reference {
	store.RLock()
	defer store.RUnlock()

	var references []reference.Reference
	for _, repository := range store.Repositories {
		for refStr, refID := range repository {
			if refID == id {
				ref, err := reference.Parse(refStr)
				if err != nil {
					// Should never happen
					return nil
				}
				references = append(references, ref)
			}
		}
	}
	return references
}

// ReferencesByName returns the references for a given repository name.
// If there are no references known for this repository name,
// ReferencesByName returns nil.
func (store *store) ReferencesByName(ref reference.Named) []Association {
	store.RLock()
	defer store.RUnlock()

	repository, exists := store.Repositories[ref.Name()]
	if !exists {
		return nil
	}

	var associations []Association
	for refStr, refID := range repository {
		ref, err := reference.Parse(refStr)
		if err != nil {
			// Should never happen
			return nil
		}
		associations = append(associations,
			Association{
				Ref:     ref,
				ImageID: refID,
			})
	}

	return associations
}

func (store *store) save() error {
	// Store the json
	jsonData, err := json.Marshal(store)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(store.jsonPath, jsonData, 0600); err != nil {
		return err
	}
	return nil
}

func (store *store) reload() error {
	f, err := os.Open(store.jsonPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&store); err != nil {
		return err
	}
	return nil
}
