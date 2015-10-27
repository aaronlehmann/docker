package daemon

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/autogen/dockerversion"
	"github.com/docker/docker/images"
	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/httputils"
	"github.com/docker/docker/pkg/progressreader"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/docker/docker/runconfig"
	"github.com/docker/docker/utils"
)

// ImportImage imports an image, getting the archived layer data either from
// inConfig (if src is "-"), or from a URI specified in src. Progress output is
// written to outStream. Repository and tag names can optionally be given in
// the repo and tag arguments, respectively.
func (daemon *Daemon) ImportImage(src, repo, tag, msg string, inConfig io.ReadCloser, outStream io.Writer, config *runconfig.Config) error {
	var (
		sf      = streamformatter.NewJSONStreamFormatter()
		archive io.ReadCloser
		resp    *http.Response
	)

	if src == "-" {
		archive = inConfig
	} else {
		inConfig.Close()
		u, err := url.Parse(src)
		if err != nil {
			return err
		}
		if u.Scheme == "" {
			u.Scheme = "http"
			u.Host = src
			u.Path = ""
		}
		outStream.Write(sf.FormatStatus("", "Downloading from %s", u))
		resp, err = httputils.Download(u.String())
		if err != nil {
			return err
		}
		progressReader := progressreader.New(progressreader.Config{
			In:        resp.Body,
			Out:       outStream,
			Formatter: sf,
			Size:      resp.ContentLength,
			NewLines:  true,
			ID:        "",
			Action:    "Importing",
		})
		archive = progressReader
	}

	defer archive.Close()
	if len(msg) == 0 {
		msg = "Imported from " + src
	}

	l, err := daemon.layerStore.Register(archive, "")
	if err != nil {
		return err
	}
	defer daemon.layerStore.Release(l)

	created := time.Now().UTC()
	imgConfig, err := json.Marshal(&images.Image{
		ImageV1: images.ImageV1{
			DockerVersion: dockerversion.VERSION,
			Config:        config,
			Architecture:  runtime.GOARCH,
			OS:            runtime.GOOS,
			Created:       created,
		},
		RootFS: &images.RootFS{
			Type:    "layers",
			DiffIDs: []layer.DiffID{l.DiffID()},
		},
		History: []images.History{{
			Created:     created,
			Description: msg,
		}},
	})
	if err != nil {
		return err
	}

	id, err := daemon.imageStore.Create(imgConfig)
	if err != nil {
		return err
	}

	// FIXME: connect with commit code and call tagstore directly
	if repo != "" {
		newRef, err := reference.WithName(repo) // todo: should move this to API layer
		if err != nil {
			return err
		}
		if tag != "" {
			if newRef, err = reference.WithTag(newRef, tag); err != nil {
				return err
			}
		}
		if err := daemon.TagImage(newRef, id.String(), true); err != nil {
			return err
		}
	}

	outStream.Write(sf.FormatStatus("", string(id)))
	logID := string(id)
	if tag != "" {
		logID = utils.ImageReference(logID, tag)
	}

	daemon.EventsService.Log("import", logID, "")
	return nil

}
