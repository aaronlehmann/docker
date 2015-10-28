package layer

import (
	"archive/tar"
	"bytes"
	"io"
)

// DigestSha256EmptyTar is the canonical sha256 digest of empty tar file -
// (1024 NULL bytes)
const DigestSha256EmptyTar = DiffID("sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef")

type emptyLayer struct{}

var EmptyLayer = &emptyLayer{}

func (el *emptyLayer) TarStream() (io.Reader, error) {
	buf := new(bytes.Buffer)
	tarWriter := tar.NewWriter(buf)
	tarWriter.Close()
	return buf, nil
}

func (el *emptyLayer) ID() ID {
	return ID(DigestSha256EmptyTar)
}

func (el *emptyLayer) DiffID() DiffID {
	return DigestSha256EmptyTar
}

func (el *emptyLayer) Parent() Layer {
	return nil
}

func (el *emptyLayer) Size() (size int64, err error) {
	return 0, nil
}

func (el *emptyLayer) DiffSize() (size int64, err error) {
	return 0, nil
}

func (cl *emptyLayer) Metadata() (map[string]string, error) {
	return make(map[string]string), nil
}
