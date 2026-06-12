package pkgcluster

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// DNSStrategy discovers cluster nodes by periodically polling DNS A records
// for a given query. It is a Go clone of libcluster's Cluster.Strategy.DNSPoll.
//
// Config keys:
//   - "query" (string) — DNS name to resolve, e.g. "my-app.example.com" (required)
//   - "node_basename" (string) — prefix used to form node addresses (required)
//   - "polling_interval" (int) — in milliseconds (default: 5000)
//   - "port" (int) — port appended to resolved IPs (default: 7000)
//
// Node addresses are formatted as <ip>:<port>.
type DNSStrategy struct {
	state State
}

// NewDNSStrategy creates a new DNSStrategy.
func NewDNSStrategy(s State) (Strategy, error) {
	return &DNSStrategy{state: s}, nil
}

// Name returns "dns".
func (d *DNSStrategy) Name() string { return "dns" }

// Run implements the poll-based discovery loop.
func (d *DNSStrategy) Run(ctx context.Context, state State) error {
	interval := pollingInterval(state)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run first load immediately.
	nodes, err := d.resolve(ctx, state)
	if err != nil {
		globalLogger.Warn("dns[%s]: initial resolution failed: %v", state.Name, err)
	} else {
		d.syncNodes(ctx, state, nodes)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nodes, err := d.resolve(ctx, state)
			if err != nil {
				globalLogger.Warn("dns[%s]: resolution failed: %v", state.Name, err)
				continue
			}
			d.syncNodes(ctx, state, nodes)
		}
	}
}

func (d *DNSStrategy) resolve(ctx context.Context, state State) ([]string, error) {
	query := StringConfig(state, "query", "")
	basename := StringConfig(state, "node_basename", "")
	if query == "" || basename == "" {
		return nil, fmt.Errorf("dns strategy requires both query and node_basename")
	}

	globalLogger.Debug("dns[%s]: resolving %s", state.Name, query)

	port := IntConfig(state, "port", 7000)

	// Use the system resolver.
	var resolver net.Resolver
	ips, err := resolver.LookupIPAddr(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("dns lookup failed for %q: %w", query, err)
	}

	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		addr := fmt.Sprintf("%s:%d", ip.IP.String(), port)
		out = append(out, addr)
	}
	return out, nil
}

func (d *DNSStrategy) syncNodes(ctx context.Context, state State, discovered []string) {
	current, err := state.ListNodes(ctx)
	if err != nil {
		globalLogger.Warn("dns[%s]: list_nodes failed: %v", state.Name, err)
		current = nil
	}

	discSet := make(map[string]bool, len(discovered))
	for _, n := range discovered {
		discSet[n] = true
	}
	currSet := make(map[string]bool, len(current))
	for _, n := range current {
		currSet[n] = true
	}

	// Disconnect removed nodes.
	for _, addr := range current {
		if !discSet[addr] {
			globalLogger.Debug("dns[%s]: disconnecting %s", state.Name, addr)
			if err := state.Disconnect(ctx, addr); err != nil {
				globalLogger.Warn("dns[%s]: disconnect %s failed: %v", state.Name, addr, err)
			}
		}
	}

	// Connect new nodes.
	for _, addr := range discovered {
		if !currSet[addr] {
			globalLogger.Debug("dns[%s]: connecting %s", state.Name, addr)
			if err := state.Connect(ctx, addr); err != nil {
				globalLogger.Warn("dns[%s]: connect %s failed: %v", state.Name, addr, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// DNS SRV strategy (for Kubernetes StatefulSets)
// ---------------------------------------------------------------------------

// DNSSRVStrategy discovers cluster nodes via DNS SRV records. It mirrors
// libcluster's Cluster.Strategy.Kubernetes.DNSSRV and is designed for
// Kubernetes StatefulSets exposed via a headless service.
//
// Config keys:
//   - "service" (string) — headless service name (required)
//   - "application_name" (string) — node name prefix (required)
//   - "namespace" (string) — Kubernetes namespace (required)
//   - "polling_interval" (int) — in milliseconds (default: 5000)
//   - "cluster_domain" (string) — Kubernetes cluster domain (default: "cluster.local")
//
// Node addresses are formatted as <fqdn>:<port> (port comes from the SRV record).
type DNSSRVStrategy struct {
	state State
}

// NewDNSSRVStrategy is an alias for NewKubernetesDNSSRVStrategy.
func NewDNSSRVStrategy(s State) (Strategy, error) { return NewKubernetesDNSSRVStrategy(s) }

// KubernetesDNSSRVStrategy is the same as DNSSRVStrategy but with a kubernetes_
// prefix in its name.
type KubernetesDNSSRVStrategy struct {
	state State
}

// NewKubernetesDNSSRVStrategy creates a new DNS SRV strategy.
func NewKubernetesDNSSRVStrategy(s State) (Strategy, error) {
	return &KubernetesDNSSRVStrategy{state: s}, nil
}

// Name returns "kubernetes_dns_srv".
func (k *KubernetesDNSSRVStrategy) Name() string { return "kubernetes_dns_srv" }

// Run implements the poll-based discovery loop.
func (k *KubernetesDNSSRVStrategy) Run(ctx context.Context, state State) error {
	interval := pollingInterval(state)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	nodes, err := k.resolve(ctx, state)
	if err != nil {
		globalLogger.Warn("dns_srv[%s]: initial resolution failed: %v", state.Name, err)
	} else {
		k.syncNodes(ctx, state, nodes)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nodes, err := k.resolve(ctx, state)
			if err != nil {
				globalLogger.Warn("dns_srv[%s]: resolution failed: %v", state.Name, err)
				continue
			}
			k.syncNodes(ctx, state, nodes)
		}
	}
}

func (k *KubernetesDNSSRVStrategy) resolve(ctx context.Context, state State) ([]string, error) {
	service := StringConfig(state, "service", "")
	appName := StringConfig(state, "application_name", "")
	namespace := StringConfig(state, "namespace", "")
	if service == "" || appName == "" || namespace == "" {
		return nil, fmt.Errorf("dns_srv strategy requires service, application_name, and namespace")
	}

	clusterDomain := StringConfig(state, "cluster_domain", "cluster.local")
	fqdn := fmt.Sprintf("%s.%s.svc.%s", service, namespace, clusterDomain)

	_, addrs, err := net.LookupSRV("", "", fqdn)
	if err != nil {
		return nil, fmt.Errorf("SRV lookup failed for %q: %w", fqdn, err)
	}

	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		target := strings.TrimSuffix(a.Target, ".")
		addr := fmt.Sprintf("%s:%d", target, a.Port)
		out = append(out, addr)
	}
	return out, nil
}

func (k *KubernetesDNSSRVStrategy) syncNodes(ctx context.Context, state State, discovered []string) {
	current, err := state.ListNodes(ctx)
	if err != nil {
		globalLogger.Warn("dns_srv[%s]: list_nodes failed: %v", state.Name, err)
		current = nil
	}

	discSet := make(map[string]bool, len(discovered))
	for _, n := range discovered {
		discSet[n] = true
	}
	currSet := make(map[string]bool, len(current))
	for _, n := range current {
		currSet[n] = true
	}

	for _, addr := range current {
		if !discSet[addr] {
			globalLogger.Debug("dns_srv[%s]: disconnecting %s", state.Name, addr)
			if err := state.Disconnect(ctx, addr); err != nil {
				globalLogger.Warn("dns_srv[%s]: disconnect %s failed: %v", state.Name, addr, err)
			}
		}
	}
	for _, addr := range discovered {
		if !currSet[addr] {
			globalLogger.Debug("dns_srv[%s]: connecting %s", state.Name, addr)
			if err := state.Connect(ctx, addr); err != nil {
				globalLogger.Warn("dns_srv[%s]: connect %s failed: %v", state.Name, addr, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Kubernetes DNS strategy (A records for headless service)
// ---------------------------------------------------------------------------

// KubernetesDNSStrategy discovers cluster nodes via DNS A records for a
// headless service. It mirrors libcluster's Cluster.Strategy.Kubernetes.DNS.
//
// Config keys:
//   - "service" (string) — headless service name (required)
//   - "application_name" (string) — node name prefix (required)
//   - "polling_interval" (int) — in milliseconds (default: 5000)
//   - "port" (int) — port appended to resolved IPs (default: 7000)
//
// Node addresses are formatted as <ip>:<port>.
type KubernetesDNSStrategy struct {
	state State
}

// NewKubernetesDNSStrategy creates a new Kubernetes DNS strategy.
func NewKubernetesDNSStrategy(s State) (Strategy, error) {
	return &KubernetesDNSStrategy{state: s}, nil
}

// Name returns "kubernetes_dns".
func (k *KubernetesDNSStrategy) Name() string { return "kubernetes_dns" }

// Run implements the poll-based discovery loop.
func (k *KubernetesDNSStrategy) Run(ctx context.Context, state State) error {
	interval := pollingInterval(state)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	nodes, err := k.resolve(ctx, state)
	if err != nil {
		globalLogger.Warn("kubernetes_dns[%s]: initial resolution failed: %v", state.Name, err)
	} else {
		k.syncNodes(ctx, state, nodes)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nodes, err := k.resolve(ctx, state)
			if err != nil {
				globalLogger.Warn("kubernetes_dns[%s]: resolution failed: %v", state.Name, err)
				continue
			}
			k.syncNodes(ctx, state, nodes)
		}
	}
}

func (k *KubernetesDNSStrategy) resolve(ctx context.Context, state State) ([]string, error) {
	service := StringConfig(state, "service", "")
	if service == "" {
		return nil, fmt.Errorf("kubernetes_dns strategy requires service")
	}

	port := IntConfig(state, "port", 7000)

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("DNS A lookup failed for %q: %w", service, err)
	}

	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		addr := fmt.Sprintf("%s:%d", ip.IP.String(), port)
		out = append(out, addr)
	}
	return out, nil
}

func (k *KubernetesDNSStrategy) syncNodes(ctx context.Context, state State, discovered []string) {
	current, err := state.ListNodes(ctx)
	if err != nil {
		globalLogger.Warn("kubernetes_dns[%s]: list_nodes failed: %v", state.Name, err)
		current = nil
	}

	discSet := make(map[string]bool, len(discovered))
	for _, n := range discovered {
		discSet[n] = true
	}
	currSet := make(map[string]bool, len(current))
	for _, n := range current {
		currSet[n] = true
	}

	for _, addr := range current {
		if !discSet[addr] {
			globalLogger.Debug("kubernetes_dns[%s]: disconnecting %s", state.Name, addr)
			if err := state.Disconnect(ctx, addr); err != nil {
				globalLogger.Warn("kubernetes_dns[%s]: disconnect %s failed: %v", state.Name, addr, err)
			}
		}
	}
	for _, addr := range discovered {
		if !currSet[addr] {
			globalLogger.Debug("kubernetes_dns[%s]: connecting %s", state.Name, addr)
			if err := state.Connect(ctx, addr); err != nil {
				globalLogger.Warn("kubernetes_dns[%s]: connect %s failed: %v", state.Name, addr, err)
			}
		}
	}
}
