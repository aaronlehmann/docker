package daemon

import (
	"strings"

	derr "github.com/docker/docker/errors"
	"github.com/docker/docker/references"
)

func (d *Daemon) imageNotExistToErrcode(err error) error {
	if dne, isDNE := err.(ErrImageDoesNotExist); isDNE {
		if strings.Contains(dne.RefOrID, "@") {
			return derr.ErrorCodeNoSuchImageHash.WithArgs(dne.RefOrID)
		}
		tag := references.DefaultRefCtx.DefaultTag()
		ref, err := references.ParseAndBindDefault(dne.RefOrID)
		if err != nil {
			return derr.ErrorCodeNoSuchImageTag.WithArgs(dne.RefOrID, tag)
		}
		if tagged, isTagged := ref.(references.BoundTagged); isTagged {
			tag = tagged.Tag()
		}
		return derr.ErrorCodeNoSuchImageTag.WithArgs(ref.Name(), tag)
	}
	return err
}
