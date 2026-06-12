package pkgcluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Kubernetes strategy
// ---------------------------------------------------------------------------

// KubernetesStrategy discovers cluster nodes via the Kubernetes API using a
// label selector. It is a Go clone of libcluster's Cluster.Strategy.Kubernetes.
//
// Config keys:
//   - "namespace" (string) — Kubernetes namespace (default: from
//     /var/run/secrets/kubernetes.io/serviceaccount/namespace or "default")
//   - "selector" (string) — label selector, e.g. "app=myapp" (required)
//   - "node_basename" (string) — prefix used to form node addresses (required)
//   - "mode" (string) — one of "ip", "hostname", "dns" (default: "ip")
//   - "ip_lookup_mode" (string) — one of "endpoints", "pods" (default: "endpoints")
//   - "service_name" (string) — required when mode is "hostname"
//   - "polling_interval" (int) — in milliseconds (default: 5000)
//   - "cluster_name" (string) — used in DNS mode (default: "cluster")
//   - "kubernetes_master" (string) — API server URL override
//   - "service_account_path" (string) — SA token path (default:
//     /var/run/secrets/kubernetes.io/serviceaccount)
//   - "port" (int) — port appended to discovered addresses (default: 7000)
//
// The strategy builds node addresses as:
//
//	mode=ip:       <pod-ip>:<port>
//	mode=hostname: <pod-fqdn>:<port>
//	mode=dns:      <dashed-ip>.<ns>.pod.<cluster>.local:<port>
type KubernetesStrategy struct {
	baseURL string
	client  *http.Client
	token   string
	state   State
}

// NewKubernetesStrategy creates a new KubernetesStrategy.
func NewKubernetesStrategy(s State) (Strategy, error) {
	saPath := StringConfig(s, "service_account_path", "/var/run/secrets/kubernetes.io/serviceaccount")
	token := readSAToken(saPath)

	// Build the API server base URL.
	master := StringConfig(s, "kubernetes_master", "kubernetes.default.svc")
	clusterDomain := os.Getenv("CLUSTER_DOMAIN")
	if clusterDomain == "" {
		clusterName := StringConfig(s, "cluster_name", "cluster")
		clusterDomain = clusterName + ".local"
	}
	baseURL := resolveMasterURL(master, clusterDomain)

	// Create HTTP client with TLS config from SA ca.crt.
	transport := httpTransportForSA(saPath)

	return &KubernetesStrategy{
		baseURL: baseURL,
		client:  &http.Client{Transport: transport, Timeout: 15 * time.Second},
		token:   token,
		state:   s,
	}, nil
}

// Name returns "kubernetes".
func (k *KubernetesStrategy) Name() string { return "kubernetes" }

// Run implements the poll-based discovery loop.
func (k *KubernetesStrategy) Run(ctx context.Context, state State) error {
	// Run the first load immediately.
	nodes, err := k.load(ctx, state)
	if err != nil {
		globalLogger.Warn("kubernetes[%s]: initial load failed: %v", state.Name, err)
	} else {
		k.syncNodes(ctx, state, nodes)
	}

	interval := pollingInterval(state)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			nodes, err := k.load(ctx, state)
			if err != nil {
				globalLogger.Warn("kubernetes[%s]: load failed: %v", state.Name, err)
				continue
			}
			k.syncNodes(ctx, state, nodes)
		}
	}
}

// syncNodes computes the connect/disconnect diff and applies it.
func (k *KubernetesStrategy) syncNodes(ctx context.Context, state State, discovered []string) {
	current, err := state.ListNodes(ctx)
	if err != nil {
		globalLogger.Warn("kubernetes[%s]: list_nodes failed: %v", state.Name, err)
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

	// Disconnect nodes that disappeared.
	for _, addr := range current {
		if !discSet[addr] {
			globalLogger.Debug("kubernetes[%s]: disconnecting %s", state.Name, addr)
			if err := state.Disconnect(ctx, addr); err != nil {
				globalLogger.Warn("kubernetes[%s]: disconnect %s failed: %v", state.Name, addr, err)
			}
		}
	}

	// Connect new nodes.
	for _, addr := range discovered {
		if !currSet[addr] {
			globalLogger.Debug("kubernetes[%s]: connecting %s", state.Name, addr)
			if err := state.Connect(ctx, addr); err != nil {
				globalLogger.Warn("kubernetes[%s]: connect %s failed: %v", state.Name, addr, err)
			}
		}
	}
}

// load queries the Kubernetes API and returns the list of discovered node
// addresses.
func (k *KubernetesStrategy) load(ctx context.Context, state State) ([]string, error) {
	namespace := k.getNamespace(state)
	basename := StringConfig(state, "node_basename", "")
	selector := StringConfig(state, "selector", "")
	mode := StringConfig(state, "mode", "ip")
	ipLookup := StringConfig(state, "ip_lookup_mode", "endpoints")
	serviceName := StringConfig(state, "service_name", "")

	if basename == "" || selector == "" {
		globalLogger.Warn("kubernetes[%s]: node_basename and selector are required", state.Name)
		return nil, nil
	}

	// Query API.
	resource := "endpoints"
	if ipLookup == "pods" {
		resource = "pods"
	}
	path := fmt.Sprintf("api/v1/namespaces/%s/%s?labelSelector=%s", namespace, resource, selector)
	body, err := k.get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("kubernetes API call failed: %w", err)
	}

	var resp k8sList
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode kubernetes response: %w", err)
	}

	var addrs []k8sAddr
	switch ipLookup {
	case "pods":
		addrs = parsePods(resp)
	default:
		addrs = parseEndpoints(resp)
	}

	clusterName := StringConfig(state, "cluster_name", "cluster")
	port := IntConfig(state, "port", 7000)
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		addr := formatK8sAddress(mode, a, basename, namespace, serviceName, clusterName, port)
		if addr != "" {
			out = append(out, addr)
		}
	}
	return out, nil
}

func (k *KubernetesStrategy) getNamespace(state State) string {
	if ns := StringConfig(state, "namespace", ""); ns != "" {
		return ns
	}
	// Fallback: read from service account.
	saPath := StringConfig(state, "service_account_path", "/var/run/secrets/kubernetes.io/serviceaccount")
	if data, err := os.ReadFile(filepath.Join(saPath, "namespace")); err == nil {
		return strings.TrimSpace(string(data))
	}
	return "default"
}

func (k *KubernetesStrategy) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", k.baseURL+"/"+path, nil)
	if err != nil {
		return nil, err
	}
	if k.token != "" {
		req.Header.Set("Authorization", "Bearer "+k.token)
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kubernetes API returned %d", resp.StatusCode)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ---------------------------------------------------------------------------
// Kubernetes API types and helpers
// ---------------------------------------------------------------------------

type k8sList struct {
	Items []json.RawMessage `json:"items"`
}

type k8sAddr struct {
	IP        string
	Namespace string
	Hostname  string
}

func parseEndpoints(resp k8sList) []k8sAddr {
	var out []k8sAddr
	for _, raw := range resp.Items {
		var ep struct {
			Subsets []struct {
				Addresses []struct {
					IP        string `json:"ip"`
					Hostname  string `json:"hostname,omitempty"`
					TargetRef *struct {
						Namespace string `json:"namespace"`
					} `json:"targetRef,omitempty"`
				} `json:"addresses"`
			} `json:"subsets"`
		}
		if err := json.Unmarshal(raw, &ep); err != nil {
			continue
		}
		for _, s := range ep.Subsets {
			for _, a := range s.Addresses {
				ns := ""
				if a.TargetRef != nil {
					ns = a.TargetRef.Namespace
				}
				out = append(out, k8sAddr{IP: a.IP, Namespace: ns, Hostname: a.Hostname})
			}
		}
	}
	return out
}

func parsePods(resp k8sList) []k8sAddr {
	var out []k8sAddr
	for _, raw := range resp.Items {
		var pod struct {
			Status struct {
				PodIP string `json:"podIP"`
			} `json:"status"`
			Metadata struct {
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Spec struct {
				Hostname string `json:"hostname,omitempty"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(raw, &pod); err != nil {
			continue
		}
		if pod.Status.PodIP == "" {
			continue
		}
		out = append(out, k8sAddr{
			IP:        pod.Status.PodIP,
			Namespace: pod.Metadata.Namespace,
			Hostname:  pod.Spec.Hostname,
		})
	}
	return out
}

func formatK8sAddress(mode string, a k8sAddr, basename, namespace, serviceName, clusterName string, port int) string {
	portSuffix := fmt.Sprintf(":%d", port)

	switch mode {
	case "ip":
		return a.IP + portSuffix
	case "hostname":
		host := a.Hostname
		if host == "" {
			return ""
		}
		return fmt.Sprintf("%s.%s.%s.svc.%s.local%s", host, serviceName, namespace, clusterName, portSuffix)
	case "dns":
		dashed := strings.ReplaceAll(a.IP, ".", "-")
		return fmt.Sprintf("%s.%s.pod.%s.local%s", dashed, namespace, clusterName, portSuffix)
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// Kubernetes in-cluster helpers
// ---------------------------------------------------------------------------

func readSAToken(saPath string) string {
	data, err := os.ReadFile(filepath.Join(saPath, "token"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func httpTransportForSA(saPath string) http.RoundTripper {
	caPath := filepath.Join(saPath, "ca.crt")
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if data, err := os.ReadFile(caPath); err == nil {
		rootPool, _ := x509.SystemCertPool()
		if rootPool == nil {
			rootPool = x509.NewCertPool()
		}
		rootPool.AppendCertsFromPEM(data)
		tlsConfig.RootCAs = rootPool
	} else {
		tlsConfig.InsecureSkipVerify = true
	}
	return &http.Transport{TLSClientConfig: tlsConfig}
}

func resolveMasterURL(master, domain string) string {
	if strings.HasPrefix(master, "https://") || strings.HasPrefix(master, "http://") {
		return strings.TrimSuffix(master, "/")
	}
	// If it already contains the domain, use as-is.
	if strings.HasSuffix(master, "."+domain) || strings.HasSuffix(master, ".") {
		return "https://" + master
	}
	return "https://" + master + "." + domain
}
