package references

// FIXME rename package to references

import (
	"fmt"
	"strings"

	"github.com/docker/distribution/digest"
	// FIXME s/distreference/reference/
	distreference "github.com/docker/distribution/reference"
	"github.com/docker/docker/image/v1"
)

// DefaultRefCtx is a reference context populated with values that make
// sense for the Docker daemon and client.
var DefaultRefCtx = RefCtx{
	defaultTag:            "latest",
	defaultHostname:       "docker.io",
	legacyDefaultHostname: "index.docker.io",
	defaultRepoPrefix:     "library/",
}

// BoundNamed is a named reference that has been bound to a configuration
// context, such as within the docker daemon. It is obtained through a
// reference context.
type BoundNamed interface {
	distreference.Named
	// WithTag combines the name from "name" and the tag from "tag" to form a
	// bound reference incorporating both the name and the tag.
	WithTag(tag string) (BoundTagged, error)
	// WithDigest combines the name from "name" and the digest from "digest"
	// to form a bound reference incorporating both the name and the digest.
	WithDigest(digest digest.Digest) (BoundCanonical, error)
	// WithDefaultTag adds a default tag to a reference if it only has a repo name.
	WithDefaultTag() BoundNamed
	// FullName returns full repository name with hostname, like "docker.io/library/ubuntu"
	FullName() string
	// Hostname returns hostname for the reference, like "docker.io"
	Hostname() string
	// RemoteName returns the repository component of the full name, like "library/ubuntu"
	RemoteName() string
}

// BoundTagged is a tagged reference that has been bound to a configuration
// context, such as within the docker daemon. It is obtained through a
// reference context.
type BoundTagged interface {
	BoundNamed
	Tag() string
}

// BoundCanonical is a digest reference that has been bound to a configuration
// context, such as within the docker daemon. It is obtained through a
// reference context.
type BoundCanonical interface {
	BoundNamed
	Digest() digest.Digest
}

// ReferenceContext takes raw Named references and binds them to the
// configured context, providing a set of extension methods useful throughout
// docker/docker.
type ReferenceContext interface {
	// Bind returns a named reference bound to the context.
	Bind(ref distreference.Named) (BoundNamed, error)
}

// RefCtx is the docker daemon and client's implementation of a
// ReferenceContext.
type RefCtx struct {
	// defaultTag defines the default tag used when performing images related actions and no tag or digest is specified
	defaultTag string
	// defaultHostname is the default built-in hostname
	defaultHostname string
	// legacyDefaultHostname is automatically converted to DefaultHostname
	legacyDefaultHostname string
	// defaultRepoPrefix is the prefix used for default repositories in default host
	defaultRepoPrefix string
}

// ParseAndBindDefault is a convenience function to parse s and returns a
// syntactically valid Named reference bound to the default reference context.
// The reference must have a name, otherwise an error is returned.
// If an error was encountered it is returned, along with a nil Bound reference.
func ParseAndBindDefault(s string) (BoundNamed, error) {
	named, err := distreference.ParseNamed(s)
	if err != nil {
		return nil, fmt.Errorf("Error parsing reference: %q is not a valid repository/tag", s)
	}
	return DefaultRefCtx.Bind(named)
}

// Bind returns a named reference bound to the context. If the input
// is invalid ErrReferenceInvalidFormat will be returned.
func (ctx RefCtx) Bind(ref distreference.Named) (BoundNamed, error) {
	name := ctx.normalize(ref)
	if err := validateName(name); err != nil {
		return nil, err
	}
	r, err := distreference.ParseNamed(name)
	if err != nil {
		return nil, err
	}
	boundRef := namedRef{Named: r, ctx: ctx}
	if canonical, isCanonical := ref.(distreference.Canonical); isCanonical {
		return boundRef.WithDigest(canonical.Digest())
	}
	if tagged, isTagged := ref.(distreference.Tagged); isTagged {
		return boundRef.WithTag(tagged.Tag())
	}
	return boundRef, nil
}

// DefaultTag defines the default tag used when performing images related actions and no tag or digest is specified.
func (ctx RefCtx) DefaultTag() string {
	return ctx.defaultTag
}

// DefaultHostname is the default built-in hostname.
func (ctx RefCtx) DefaultHostname() string {
	return ctx.defaultHostname
}

// LegacyDefaultHostname is automatically converted to DefaultHostname.
func (ctx RefCtx) LegacyDefaultHostname() string {
	return ctx.legacyDefaultHostname
}

// DefaultRepoPrefix is the prefix used for default repositories in default host.
func (ctx RefCtx) DefaultRepoPrefix() string {
	return ctx.defaultRepoPrefix
}

type namedRef struct {
	distreference.Named
	ctx RefCtx
}

type taggedRef struct {
	namedRef
}

type canonicalRef struct {
	namedRef
}

func (r namedRef) FullName() string {
	hostname, remoteName := r.ctx.splitHostname(r)
	return hostname + "/" + remoteName
}

func (r namedRef) Hostname() string {
	hostname, _ := r.ctx.splitHostname(r)
	return hostname
}

func (r namedRef) RemoteName() string {
	_, remoteName := r.ctx.splitHostname(r)
	return remoteName
}

// WithTag combines the name from "name" and the tag from "tag" to form a
// bound reference incorporating both the name and the tag.
func (name namedRef) WithTag(tag string) (BoundTagged, error) {
	r, err := distreference.WithTag(name, tag)
	if err != nil {
		return nil, err
	}
	return taggedRef{namedRef{Named: r, ctx: name.ctx}}, nil
}

// WithDigest combines the name from "name" and the digest from "digest"
// to form a bound reference incorporating both the name and the digest.
func (name namedRef) WithDigest(digest digest.Digest) (BoundCanonical, error) {
	r, err := distreference.WithDigest(name, digest)
	if err != nil {
		return nil, err
	}
	return canonicalRef{namedRef{Named: r, ctx: name.ctx}}, nil
}

// WithDefaultTag adds a default tag to a reference if it only has a repo name.
func (r namedRef) WithDefaultTag() BoundNamed {
	ref, _ := r.WithTag(r.ctx.DefaultTag())
	return ref
}

func (r taggedRef) Tag() string {
	return r.namedRef.Named.(distreference.Tagged).Tag()
}

// WithDefaultTag adds a default tag to a reference if it only has a repo name.
func (r taggedRef) WithDefaultTag() BoundNamed {
	return r
}

func (r canonicalRef) Digest() digest.Digest {
	return r.namedRef.Named.(distreference.Canonical).Digest()
}

// WithDefaultTag adds a default tag to a reference if it only has a repo name.
func (r canonicalRef) WithDefaultTag() BoundNamed {
	return r
}

// splitHostname splits a repository name to hostname and remotename string.
// If no valid hostname is found, the default hostname is used. Repository name
// needs to be already validated before.
func (ctx RefCtx) splitHostname(name distreference.Named) (hostname, remoteName string) {
	hostname, remoteName = distreference.SplitHostname(name)
	if hostname == "" || (!strings.ContainsAny(hostname, ".:") && hostname != "localhost") {
		hostname, remoteName = ctx.defaultHostname, name.Name()
	}
	if hostname == ctx.legacyDefaultHostname {
		hostname = ctx.defaultHostname
	}
	if hostname == ctx.defaultHostname && !strings.ContainsRune(remoteName, '/') {
		remoteName = ctx.defaultRepoPrefix + remoteName
	}
	return
}

// normalize returns a repository name in its normalized form, meaning it
// will not contain default hostname nor library/ prefix for official images.
func (ctx RefCtx) normalize(name distreference.Named) string {
	host, remoteName := ctx.splitHostname(name)
	if host == ctx.defaultHostname {
		if strings.HasPrefix(remoteName, ctx.defaultRepoPrefix) {
			return strings.TrimPrefix(remoteName, ctx.defaultRepoPrefix)
		}
		return remoteName
	}
	return name.Name()
}

func validateName(name string) error {
	if err := v1.ValidateID(name); err == nil {
		return fmt.Errorf("Invalid repository name (%s), cannot specify 64-byte hexadecimal strings", name)
	}
	return nil
}
