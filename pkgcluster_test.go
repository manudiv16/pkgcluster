package pkgcluster

import (
	"context"
	"fmt"
	"testing"
)

func TestNode_NewNode(t *testing.T) {
	node := NewNode("127.0.0.1:7000", map[string]string{
		"region": "us-east-1",
		"zone":   "a",
	})

	if node.ID != "127.0.0.1:7000" {
		t.Errorf("Node.ID = %s, want '127.0.0.1:7000'", node.ID)
	}
	if node.Address != "127.0.0.1:7000" {
		t.Errorf("Node.Address = %s, want '127.0.0.1:7000'", node.Address)
	}
	if len(node.Meta) != 2 {
		t.Errorf("Node.Meta length = %d, want 2", len(node.Meta))
	}
	if node.Meta["region"] != "us-east-1" {
		t.Errorf("Node.Meta[region] = %s, want 'us-east-1'", node.Meta["region"])
	}
}

func TestNode_NewNodeWithID(t *testing.T) {
	node := NewNodeWithID("node-1", "10.0.0.1:7000", nil)

	if node.ID != "node-1" {
		t.Errorf("Node.ID = %s, want 'node-1'", node.ID)
	}
	if node.Address != "10.0.0.1:7000" {
		t.Errorf("Node.Address = %s, want '10.0.0.1:7000'", node.Address)
	}
	if node.Meta == nil {
		t.Error("Node.Meta should be non-nil after NewNodeWithID")
	}
}

func TestStrategyType_Constants(t *testing.T) {
	tests := []struct {
		name     string
		strategy StrategyType
		expected string
	}{
		{"static", StrategyStatic, "static"},
		{"kubernetes", StrategyKubernetes, "kubernetes"},
		{"kubernetes_dns", StrategyKubernetesDNS, "kubernetes_dns"},
		{"kubernetes_dns_srv", StrategyKubernetesDNSSRV, "kubernetes_dns_srv"},
		{"dns", StrategyDNS, "dns"},
		{"gossip", StrategyGossip, "gossip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.strategy) != tt.expected {
				t.Errorf("Strategy %s = %s, want %s", tt.name, string(tt.strategy), tt.expected)
			}
		})
	}
}

func TestEvent_Structure(t *testing.T) {
	node := NewNode("192.168.1.100:7000", nil)

	tests := []struct {
		name      string
		eventType EventType
		expected  string
	}{
		{"node joined", EventNodeJoined, "node_joined"},
		{"node left", EventNodeLeft, "node_left"},
		{"node failed", EventNodeFailed, "node_failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := Event{
				Type: tt.eventType,
				Node: node,
			}

			if string(event.Type) != tt.expected {
				t.Errorf("Event.Type = %s, want %s", string(event.Type), tt.expected)
			}
			if event.Node.ID != node.ID {
				t.Errorf("Event.Node.ID = %s, want %s", event.Node.ID, node.ID)
			}
		})
	}
}

func TestEventType_Constants(t *testing.T) {
	if string(EventNodeJoined) != "node_joined" {
		t.Errorf("EventNodeJoined = %s, want 'node_joined'", string(EventNodeJoined))
	}
	if string(EventNodeLeft) != "node_left" {
		t.Errorf("EventNodeLeft = %s, want 'node_left'", string(EventNodeLeft))
	}
	if string(EventNodeFailed) != "node_failed" {
		t.Errorf("EventNodeFailed = %s, want 'node_failed'", string(EventNodeFailed))
	}
}

func TestNewStrategy_ErrorCases(t *testing.T) {
	noopState := State{
		Name: "test",
		Config: map[string]interface{}{},
		Connect:    func(ctx context.Context, addr string) error { return nil },
		Disconnect: func(ctx context.Context, addr string) error { return nil },
		ListNodes:  func(ctx context.Context) ([]string, error) { return nil, nil },
	}

	tests := []struct {
		name         string
		strategyType StrategyType
		wantErr      bool
	}{
		{"static strategy", StrategyStatic, false},
		{"unknown strategy", StrategyType("unknown"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := NewStrategy(tt.strategyType, noopState)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if strategy != nil {
					t.Error("Expected nil strategy when error occurs")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if strategy == nil {
					t.Error("Expected strategy but got nil")
				}
			}
		})
	}
}

func TestErrors(t *testing.T) {
	if ErrUnknownStrategy == nil {
		t.Error("ErrUnknownStrategy should not be nil")
	}
	if ErrNotImplemented == nil {
		t.Error("ErrNotImplemented should not be nil")
	}

	if ErrUnknownStrategy.Error() != "unknown discovery strategy" {
		t.Errorf("ErrUnknownStrategy message = %s, want 'unknown discovery strategy'",
			ErrUnknownStrategy.Error())
	}
	if ErrNotImplemented.Error() != "strategy not implemented" {
		t.Errorf("ErrNotImplemented message = %s, want 'strategy not implemented'",
			ErrNotImplemented.Error())
	}
}

func TestStatefulSetDNSName(t *testing.T) {
	tests := []struct {
		name        string
		podName     string
		serviceName string
		namespace   string
		domain      string
		port        int
		expected    string
	}{
		{
			name:        "with port",
			podName:     "app-0",
			serviceName: "app",
			namespace:   "default",
			domain:      "cluster.local",
			port:        7000,
			expected:    "app-0.app.default.svc.cluster.local:7000",
		},
		{
			name:        "without port",
			podName:     "app-1",
			serviceName: "app",
			namespace:   "default",
			domain:      "cluster.local",
			port:        0,
			expected:    "app-1.app.default.svc.cluster.local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StatefulSetDNSName(tt.podName, tt.serviceName, tt.namespace, tt.domain, tt.port)
			if result != tt.expected {
				t.Errorf("StatefulSetDNSName = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestNewStrategy_CreatesConcreteTypes(t *testing.T) {
	noopState := State{
		Name: "test",
		Config: map[string]interface{}{},
		Connect:    func(ctx context.Context, addr string) error { return nil },
		Disconnect: func(ctx context.Context, addr string) error { return nil },
		ListNodes:  func(ctx context.Context) ([]string, error) { return nil, nil },
	}

	type testCase struct {
		strategy StrategyType
		expect   string
	}
	tests := []testCase{
		{StrategyStatic, "*pkgcluster.StaticStrategy"},
		{StrategyDNS, "*pkgcluster.DNSStrategy"},
		{StrategyKubernetes, "*pkgcluster.KubernetesStrategy"},
		{StrategyKubernetesDNS, "*pkgcluster.KubernetesDNSStrategy"},
		{StrategyKubernetesDNSSRV, "*pkgcluster.KubernetesDNSSRVStrategy"},
		{StrategyGossip, "*pkgcluster.GossipStrategy"},
	}

	for _, tt := range tests {
		t.Run(string(tt.strategy), func(t *testing.T) {
			s, err := NewStrategy(tt.strategy, noopState)
			if err != nil {
				t.Fatalf("NewStrategy(%q) unexpected error: %v", tt.strategy, err)
			}
			got := fmt.Sprintf("%T", s)
			if got != tt.expect {
				t.Errorf("expected type %s, got %s", tt.expect, got)
			}
		})
	}
}

func TestIntConfig(t *testing.T) {
	s := State{
		Config: map[string]interface{}{
			"port":   7000,
			"string": "hello",
		},
	}

	if got := IntConfig(s, "port", 0); got != 7000 {
		t.Errorf("IntConfig(port) = %d, want 7000", got)
	}
	if got := IntConfig(s, "missing", 42); got != 42 {
		t.Errorf("IntConfig(missing) = %d, want 42", got)
	}
}

func TestStringConfig(t *testing.T) {
	s := State{
		Config: map[string]interface{}{
			"mode": "ip",
		},
	}

	if got := StringConfig(s, "mode", ""); got != "ip" {
		t.Errorf("StringConfig(mode) = %s, want 'ip'", got)
	}
	if got := StringConfig(s, "missing", "default"); got != "default" {
		t.Errorf("StringConfig(missing) = %s, want 'default'", got)
	}
}
