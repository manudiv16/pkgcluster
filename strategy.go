// Package pkgcluster provides a pluggable cluster membership system inspired by
// libcluster (https://github.com/bitwalker/libcluster). It discovers cluster
// nodes through configurable strategies and delegates connect/disconnect
// decisions to the caller via callbacks.
//
// Architecture:
//   - A Manager runs one or more Topologies, each backed by a Strategy.
//   - Each Strategy runs as a long-lived goroutine that periodically polls for
//     nodes (or watches for changes) and calls the Connect/Disconnect callbacks
//     supplied in its State.
//   - Strategies are decoupled from any specific transport (Raft, HTTP, etc.)
//     — they only discover node addresses and let the caller decide how to
//     connect or disconnect.
package pkgcluster

import (
	"context"
	"fmt"
)

// Strategy defines the interface for cluster discovery strategies.
// Each strategy runs as a long-lived background loop until its context
// is cancelled. It uses the callbacks in State to notify the caller
// about nodes to connect to or disconnect from.
type Strategy interface {
	// Run starts the discovery loop. It should block until ctx is done
	// or an unrecoverable error occurs.
	Run(ctx context.Context, s State) error

	// Name returns a human-readable name for this strategy.
	Name() string
}

// State is passed to a Strategy at start time and provides topology-level
// configuration and callbacks.
type State struct {
	// Name is the topology name (used for logging/diagnostics).
	Name string

	// Config holds strategy-specific key-value configuration.
	Config map[string]interface{}

	// Connect is called when the strategy determines a node should be
	// added to the cluster. addr is the node's address string.
	// Returning an error logs a warning but does not stop the strategy.
	Connect func(ctx context.Context, addr string) error

	// Disconnect is called when the strategy determines a node should be
	// removed from the cluster. addr is the node's address string.
	Disconnect func(ctx context.Context, addr string) error

	// ListNodes returns the list of currently connected node addresses.
	// Used to compute the diff of what to connect/disconnect.
	ListNodes func(ctx context.Context) ([]string, error)
}

// Node represents a discovered node with its address and optional metadata.
type Node struct {
	ID      string            `json:"id"`
	Address string            `json:"address"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// NewNode creates a Node with the given address and optional key-value pairs
// as metadata.
func NewNode(address string, meta map[string]string) Node {
	if meta == nil {
		meta = make(map[string]string)
	}
	return Node{
		ID:      address,
		Address: address,
		Meta:    meta,
	}
}

// NewNodeWithID creates a Node with a separate identity and address.
// This is useful when the node ID differs from its address (e.g. a
// StatefulSet pod name vs its IP).
func NewNodeWithID(id, address string, meta map[string]string) Node {
	if meta == nil {
		meta = make(map[string]string)
	}
	return Node{
		ID:      id,
		Address: address,
		Meta:    meta,
	}
}

// StrategyType identifies a built-in discovery strategy.
type StrategyType string

const (
	StrategyStatic           StrategyType = "static"
	StrategyKubernetes       StrategyType = "kubernetes"
	StrategyKubernetesDNS    StrategyType = "kubernetes_dns"
	StrategyKubernetesDNSSRV StrategyType = "kubernetes_dns_srv"
	StrategyDNS              StrategyType = "dns"
	StrategyGossip           StrategyType = "gossip"
)

// NewStrategy instantiates a Strategy by type.
func NewStrategy(t StrategyType, s State) (Strategy, error) {
	switch t {
	case StrategyStatic:
		return NewStaticStrategy(s)
	case StrategyKubernetes:
		return NewKubernetesStrategy(s)
	case StrategyKubernetesDNS:
		return NewKubernetesDNSStrategy(s)
	case StrategyKubernetesDNSSRV:
		return NewKubernetesDNSSRVStrategy(s)
	case StrategyDNS:
		return NewDNSStrategy(s)
	case StrategyGossip:
		return NewGossipStrategy(s)
	default:
		return nil, fmt.Errorf("unknown discovery strategy: %q", t)
	}
}

// StatefulSetDNSName returns the Kubernetes FQDN for a StatefulSet pod.
// Format: <pod-name>.<service-name>.<namespace>.svc.<domain>[:<port>]
// Port is appended only when port > 0.
func StatefulSetDNSName(podName, serviceName, namespace, domain string, port int) string {
	if domain == "" {
		domain = "cluster.local"
	}
	fqdn := fmt.Sprintf("%s.%s.%s.svc.%s", podName, serviceName, namespace, domain)
	if port > 0 {
		return fmt.Sprintf("%s:%d", fqdn, port)
	}
	return fqdn
}

// Event represents a cluster membership event.
type Event struct {
	Type EventType `json:"type"`
	Node Node      `json:"node"`
}

// EventType categorises a cluster membership change.
type EventType string

const (
	EventNodeJoined EventType = "node_joined"
	EventNodeLeft   EventType = "node_left"
	EventNodeFailed EventType = "node_failed"
)

// Sentinel errors.
var (
	ErrUnknownStrategy = fmt.Errorf("unknown discovery strategy")
	ErrNotImplemented  = fmt.Errorf("strategy not implemented")
)
