package client

import (
	"errors"
	"fmt"

	"github.com/docker/distribution/reference"
	Cli "github.com/docker/docker/cli"
	"github.com/docker/docker/pkg/jsonmessage"
	flag "github.com/docker/docker/pkg/mflag"
	"github.com/docker/docker/references"
	"github.com/docker/docker/registry"
	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
)

// CmdPull pulls an image or a repository from the registry.
//
// Usage: docker pull [OPTIONS] IMAGENAME[:TAG|@DIGEST]
func (cli *DockerCli) CmdPull(args ...string) error {
	cmd := Cli.Subcmd("pull", []string{"NAME[:TAG|@DIGEST]"}, Cli.DockerCommands["pull"].Description, true)
	allTags := cmd.Bool([]string{"a", "-all-tags"}, false, "Download all tagged images in the repository")
	addTrustedFlags(cmd, true)
	cmd.Require(flag.Exact, 1)

	cmd.ParseFlags(args, true)
	remote := cmd.Arg(0)

	distributionRef, err := references.ParseAndBindDefault(remote)
	if err != nil {
		return err
	}
	if *allTags && !reference.NamedOnly(distributionRef) {
		return errors.New("tag can't be used with --all-tags/-a")
	}

	if !*allTags && reference.NamedOnly(distributionRef) {
		distributionRef = distributionRef.WithDefaultTag()
		fmt.Fprintf(cli.out, "Using default tag: %s\n", references.DefaultRefCtx.DefaultTag())
	}

	var tag string
	switch x := distributionRef.(type) {
	case references.BoundCanonical:
		tag = x.Digest().String()
	case references.BoundTagged:
		tag = x.Tag()
	}

	ref := registry.ParseReference(tag)

	// Resolve the Repository name from fqn to RepositoryInfo
	repoInfo, err := registry.ParseRepositoryInfo(distributionRef)
	if err != nil {
		return err
	}

	authConfig := registry.ResolveAuthConfig(cli.configFile.AuthConfigs, repoInfo.Index)
	requestPrivilege := cli.registryAuthenticationPrivilegedFunc(repoInfo.Index, "pull")

	if isTrusted() && !ref.HasDigest() {
		// Check if tag is digest
		return cli.trustedPull(repoInfo, ref, authConfig, requestPrivilege)
	}

	return cli.imagePullPrivileged(authConfig, distributionRef.String(), "", requestPrivilege)
}

func (cli *DockerCli) imagePullPrivileged(authConfig types.AuthConfig, imageID, tag string, requestPrivilege client.RequestPrivilegeFunc) error {

	encodedAuth, err := encodeAuthToBase64(authConfig)
	if err != nil {
		return err
	}
	options := types.ImagePullOptions{
		ImageID:      imageID,
		Tag:          tag,
		RegistryAuth: encodedAuth,
	}

	responseBody, err := cli.client.ImagePull(options, requestPrivilege)
	if err != nil {
		return err
	}
	defer responseBody.Close()

	return jsonmessage.DisplayJSONMessagesStream(responseBody, cli.out, cli.outFd, cli.isTerminalOut, nil)
}
