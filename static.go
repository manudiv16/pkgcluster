package pkgcluster

import (
	"context"
	"strings"
	"time"
)

// StaticStrategy is a discovery strategy that uses a pre-configured static
// list of node addresses. It corresponds to libcluster's ErlangHosts strategy
// but for a static address list rather than a .hosts.erlang file.
//
// Config keys:
//   - "addresses" ([]string) — the static list of node addresses to connect to.
type StaticStrategy struct {
	state State
}

// NewStaticStrategy creates a StaticStrategy.
func NewStaticStrategy(s State) (Strategy, error) {
	return &StaticStrategy{state: s}, nil
}

// Name returns "static".
func (s *StaticStrategy) Name() string { return "static" }

// Run connects to the configured static address list once, then blocks
// until ctx is cancelled.
func (s *StaticStrategy) Run(ctx context.Context, state State) error {
	addrs := getAddresses(state)
	if len(addrs) == 0 {
		globalLogger.Info("static[%s]: no addresses configured, waiting idle", state.Name)
		<-ctx.Done()
		return ctx.Err()
	}

	globalLogger.Info("static[%s]: connecting to %d configured peer(s)", state.Name, len(addrs))

	// Get currently connected nodes.
	current, _ := state.ListNodes(ctx)

	// Compute diff: connect new nodes, disconnect removed (none for static).
	for _, addr := range addrs {
		if !contains(current, addr) {
			globalLogger.Debug("static[%s]: connecting %s", state.Name, addr)
			if err := state.Connect(ctx, addr); err != nil {
				globalLogger.Warn("static[%s]: failed to connect %s: %v", state.Name, addr, err)
			}
		}
	}

	// Static strategy: no polling needed. Block until shutdown.
	<-ctx.Done()
	return ctx.Err()
}

// getAddresses extracts the address list from the config. It supports:
//   - "addresses" ([]string) — as a direct slice of strings
//   - "addresses" (string) — comma-separated list
//   - "peers" (string) — comma-separated (legacy)
func getAddresses(state State) []string {
	if v, ok := state.Config["addresses"]; ok {
		switch val := v.(type) {
		case []string:
			return val
		case string:
			return splitTrim(val)
		}
	}
	if v, ok := state.Config["peers"]; ok {
		if s, ok := v.(string); ok {
			return splitTrim(s)
		}
	}
	return nil
}

func splitTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

// pollingInterval returns the configured polling interval in milliseconds,
// or the default. It is used by poll-based strategies.
func pollingInterval(state State) time.Duration {
	if v := IntConfig(state, "polling_interval", 0); v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	if v := IntConfig(state, "poll_interval", 0); v > 0 {
		return time.Duration(v) * time.Millisecond
	}
	return 5 * time.Second
}
