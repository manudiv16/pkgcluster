package pkgcluster

import (
	"context"
)

// Topology defines a named cluster topology backed by a discovery strategy.
// It is the public configuration unit: you create one or more Topology values
// and pass them to NewManager.
//
// Example:
//
//	topo := pkgcluster.Topology{
//	    Name:     "my-cluster",
//	    Strategy: pkgcluster.StrategyKubernetes,
//	    Config: map[string]interface{}{
//	        "namespace":    "default",
//	        "selector":     "app=myapp",
//	        "node_basename": "myapp",
//	        "mode":         "ip",
//	    },
//	    Connect: func(ctx context.Context, addr string) error {
//	        return myRaft.AddPeer(addr)
//	    },
//	    Disconnect: func(ctx context.Context, addr string) error {
//	        return myRaft.RemovePeer(addr)
//	    },
//	    ListNodes: func(ctx context.Context) ([]string, error) {
//	        return myRaft.GetPeerAddresses()
//	    },
//	}
type Topology struct {
	// Name is used for logging and diagnostics.
	Name string

	// Strategy selects the built-in discovery strategy.
	Strategy StrategyType

	// Config holds strategy-specific options. See the documentation of
	// each strategy for the supported keys.
	Config map[string]interface{}

	// Connect is called when the strategy discovers a new node that should
	// be added to the cluster.
	Connect func(ctx context.Context, addr string) error

	// Disconnect is called when the strategy determines a node should be
	// removed from the cluster.
	Disconnect func(ctx context.Context, addr string) error

	// ListNodes returns the currently connected node addresses so the
	// strategy can compute the connect/disconnect diff.
	ListNodes func(ctx context.Context) ([]string, error)
}

// initialState converts a Topology into the State value passed to a Strategy.
func (t Topology) initialState() State {
	if t.Config == nil {
		t.Config = make(map[string]interface{})
	}
	return State{
		Name:       t.Name,
		Config:     t.Config,
		Connect:    t.Connect,
		Disconnect: t.Disconnect,
		ListNodes:  t.ListNodes,
	}
}

// IntConfig reads a key from s.Config and returns its int value, or def if
// the key is missing or the wrong type.
func IntConfig(s State, key string, def int) int {
	if v, ok := s.Config[key]; ok {
		if i, ok := v.(int); ok {
			return i
		}
	}
	return def
}

// StringConfig reads a key from s.Config and returns its string value, or def
// if the key is missing or the wrong type.
func StringConfig(s State, key string, def string) string {
	if v, ok := s.Config[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return def
}

// BoolConfig reads a key from s.Config and returns its bool value, or def if
// the key is missing or the wrong type.
func BoolConfig(s State, key string, def bool) bool {
	if v, ok := s.Config[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return def
}

// DurationConfig is a helper that reads a key from s.Config and returns its
// Duration value in milliseconds, or def if the key is missing or the wrong
// type.
func DurationConfig(s State, key string, def int) int {
	return IntConfig(s, key, def)
}

// StringConfigFromMap is like StringConfig but works on a plain map.
func StringConfigFromMap(m map[string]interface{}, key string, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return def
}

// IntConfigFromMap is like IntConfig but works on a plain map.
func IntConfigFromMap(m map[string]interface{}, key string, def int) int {
	if m == nil {
		return def
	}
	if v, ok := m[key]; ok {
		if i, ok := v.(int); ok {
			return i
		}
	}
	return def
}
