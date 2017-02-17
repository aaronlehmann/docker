package service

import (
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/docker/docker/api/types/swarm"
)

// FIXME(aaronl): Checking constraints on the client side is bad. We
// should provide a way to find out whether a global service is
// converged without having all this logic duplicated on the client
// side.

const (
	eq = iota
	noteq

	// nodeLabelPrefix is the constraint key prefix for node labels.
	nodeLabelPrefix = "node.labels."
	// engineLabelPrefix is the constraint key prefix for engine labels.
	engineLabelPrefix = "engine.labels."
)

var (
	alphaNumeric = regexp.MustCompile(`^(?i)[a-z_][a-z0-9\-_.]+$`)
	// value can be alphanumeric and some special characters. it shouldn't container
	// current or future operators like '>, <, ~', etc.
	valuePattern = regexp.MustCompile(`^(?i)[a-z0-9:\-_\s\.\*\(\)\?\+\[\]\\\^\$\|\/]+$`)

	// operators defines list of accepted operators
	operators = []string{"==", "!="}
)

// constraint defines a constraint.
type constraint struct {
	key      string
	operator int
	exp      string
}

// parseConstraints parses list of constraints.
func parseConstraints(env []string) ([]constraint, error) {
	exprs := []constraint{}
	for _, e := range env {
		found := false
		// each expr is in the form of "key op value"
		for i, op := range operators {
			if !strings.Contains(e, op) {
				continue
			}
			// split with the op
			parts := strings.SplitN(e, op, 2)

			if len(parts) < 2 {
				return nil, fmt.Errorf("invalid expr: %s", e)
			}

			part0 := strings.TrimSpace(parts[0])
			// validate key
			matched := alphaNumeric.MatchString(part0)
			if matched == false {
				return nil, fmt.Errorf("key '%s' is invalid", part0)
			}

			part1 := strings.TrimSpace(parts[1])

			// validate Value
			matched = valuePattern.MatchString(part1)
			if matched == false {
				return nil, fmt.Errorf("value '%s' is invalid", part1)
			}
			// TODO(dongluochen): revisit requirements to see if globing or regex are useful
			exprs = append(exprs, constraint{key: part0, operator: i, exp: part1})

			found = true
			break // found an op, move to next entry
		}
		if !found {
			return nil, fmt.Errorf("constraint expected one operator from %s", strings.Join(operators, ", "))
		}
	}
	return exprs, nil
}

// match checks if the constraint matches the target strings.
func (c *constraint) match(whats ...string) bool {
	var match bool

	// full string match
	for _, what := range whats {
		// case insensitive compare
		if strings.EqualFold(c.exp, what) {
			match = true
			break
		}
	}

	switch c.operator {
	case eq:
		return match
	case noteq:
		return !match
	}

	return false
}

// nodeMatches returns true if the node satisfies the given constraints.
func nodeMatches(constraints []constraint, n swarm.Node) bool {
	for _, constraint := range constraints {
		switch {
		case strings.EqualFold(constraint.key, "node.id"):
			if !constraint.match(n.ID) {
				return false
			}
		case strings.EqualFold(constraint.key, "node.hostname"):
			if !constraint.match(n.Description.Hostname) {
				return false
			}
		case strings.EqualFold(constraint.key, "node.ip"):
			nodeIP := net.ParseIP(n.Status.Addr)
			// single IP address, node.ip == 2001:db8::2
			if ip := net.ParseIP(constraint.exp); ip != nil {
				ipEq := ip.Equal(nodeIP)
				if (ipEq && constraint.operator != eq) || (!ipEq && constraint.operator == eq) {
					return false
				}
				continue
			}
			// CIDR subnet, node.ip != 210.8.4.0/24
			if _, subnet, err := net.ParseCIDR(constraint.exp); err == nil {
				within := subnet.Contains(nodeIP)
				if (within && constraint.operator != eq) || (!within && constraint.operator == eq) {
					return false
				}
				continue
			}
			// reject constraint with malformed address/network
			return false
		case strings.EqualFold(constraint.key, "node.role"):
			if !constraint.match(string(n.Spec.Role)) {
				return false
			}
		case strings.EqualFold(constraint.key, "node.platform.os"):
			if !constraint.match(n.Description.Platform.OS) {
				return false
			}
		case strings.EqualFold(constraint.key, "node.platform.arch"):
			if !constraint.match(n.Description.Platform.Architecture) {
				return false
			}

		// node labels constraint in form like 'node.labels.key==value'
		case len(constraint.key) > len(nodeLabelPrefix) && strings.EqualFold(constraint.key[:len(nodeLabelPrefix)], nodeLabelPrefix):
			if n.Spec.Annotations.Labels == nil {
				if !constraint.match("") {
					return false
				}
				continue
			}
			label := constraint.key[len(nodeLabelPrefix):]
			// label itself is case sensitive
			val := n.Spec.Annotations.Labels[label]
			if !constraint.match(val) {
				return false
			}

		// engine labels constraint in form like 'engine.labels.key!=value'
		case len(constraint.key) > len(engineLabelPrefix) && strings.EqualFold(constraint.key[:len(engineLabelPrefix)], engineLabelPrefix):
			if n.Description.Engine.Labels == nil {
				if !constraint.match("") {
					return false
				}
				continue
			}
			label := constraint.key[len(engineLabelPrefix):]
			val := n.Description.Engine.Labels[label]
			if !constraint.match(val) {
				return false
			}
		default:
			// key doesn't match predefined syntax
			return false
		}
	}

	return true
}
