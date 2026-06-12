# pkgcluster

[![Go Reference](https://pkg.go.dev/badge/github.com/manudiv16/pkgcluster.svg)](https://pkg.go.dev/github.com/manudiv16/pkgcluster)
[![CI](https://github.com/manudiv16/pkgcluster/actions/workflows/ci.yml/badge.svg)](https://github.com/manudiv16/pkgcluster/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/manudiv16/pkgcluster)](https://goreportcard.com/report/github.com/manudiv16/pkgcluster)

**pkgcluster** is a Go library for automatic cluster membership discovery, inspired by [libcluster](https://github.com/bitwalker/libcluster) for Elixir/Erlang.

It provides a pluggable **strategy** system — each strategy runs as a background goroutine, discovers peer nodes, and calls user-supplied callbacks to **connect** or **disconnect** them. The library is transport-agnostic: it discovers node addresses and lets **you** decide how to wire them (Raft, HTTP, custom protocol, etc.).

## Features

- 🔌 **Pluggable strategies** — static, Kubernetes API, DNS, DNS SRV, multicast gossip
- 🧩 **Transport-agnostic** — provides node addresses to your callbacks, you handle the connection
- 🏗️ **Topology-based** — run multiple discovery strategies simultaneously
- 🔒 **Gossip encryption** — optional AES-256-CBC secret for heartbeat messages
- 📦 **Zero external dependencies** (except `golang.org/x/net` for multicast) — the Kubernetes strategy uses only the standard library

## Installation

```bash
go get github.com/manudiv16/pkgcluster
```

## Quick Start

### Static Strategy

```go
package main

import (
    "context"
    "fmt"
    "github.com/manudiv16/pkgcluster"
)

func main() {
    topo := pkgcluster.Topology{
        Name:     "my-cluster",
        Strategy: pkgcluster.StrategyStatic,
        Config: map[string]interface{}{
            "addresses": []string{"10.0.0.1:7000", "10.0.0.2:7000"},
        },
        Connect: func(ctx context.Context, addr string) error {
            fmt.Println("connect to", addr)
            return nil
        },
        Disconnect: func(ctx context.Context, addr string) error {
            fmt.Println("disconnect from", addr)
            return nil
        },
        ListNodes: func(ctx context.Context) ([]string, error) {
            return []string{"10.0.0.1:7000"}, nil
        },
    }

    mgr := pkgcluster.NewManager(topo)
    mgr.Start(context.Background())
    // ... wait for shutdown ...
    mgr.Stop()
}
```

### Kubernetes Strategy

```go
topo := pkgcluster.Topology{
    Name:     "k8s-cluster",
    Strategy: pkgcluster.StrategyKubernetes,
    Config: map[string]interface{}{
        "namespace":      "default",
        "selector":       "app=myapp",
        "node_basename":  "myapp",
        "mode":           "ip",
        "port":           7000,
        "polling_interval": 5000,
    },
    // ... connect/disconnect callbacks ...
}
```

## Strategies

| Strategy | Type Constant | Description | Config Keys |
|---|---|---|---|
| **Static** | `StrategyStatic` | Pre-configured list of peer addresses | `addresses` ([]string) |
| **Kubernetes** | `StrategyKubernetes` | Queries K8s API via label selector | `namespace`, `selector`, `node_basename`, `mode`, `ip_lookup_mode` |
| **Kubernetes DNS** | `StrategyKubernetesDNS` | DNS A records for headless service | `service`, `application_name` |
| **Kubernetes DNS SRV** | `StrategyKubernetesDNSSRV` | DNS SRV for StatefulSets | `service`, `application_name`, `namespace` |
| **DNS** | `StrategyDNS` | Generic DNS A record polling | `query`, `node_basename` |
| **Gossip** | `StrategyGossip` | UDP multicast heartbeat discovery | `port`, `multicast_addr`, `secret` |

## Custom Logger

By default, pkgcluster logs to stderr via `log.Printf`. Wire your own logger:

```go
pkgcluster.SetLogger(pkgcluster.Logger{
    Debug: func(msg string, args ...any) { myLogger.Debug(fmt.Sprintf(msg, args...)) },
    Info:  func(msg string, args ...any) { myLogger.Info(fmt.Sprintf(msg, args...)) },
    Warn:  func(msg string, args ...any) { myLogger.Warn(fmt.Sprintf(msg, args...)) },
    Error: func(msg string, args ...any) { myLogger.Error(fmt.Sprintf(msg, args...)) },
})
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│                   Manager                        │
│  ┌─────────────────┐  ┌─────────────────┐       │
│  │  Topology A      │  │  Topology B      │       │
│  │  ┌─────────────┐ │  │  ┌─────────────┐ │       │
│  │  │ Kubernetes  │ │  │  │  Gossip     │ │       │
│  │  │ Strategy    │ │  │  │  Strategy   │ │       │
│  │  └──────┬──────┘ │  │  └──────┬──────┘ │       │
│  │         │        │  │         │        │       │
│  │    ┌────▼────┐   │  │    ┌────▼────┐   │       │
│  │    │Connect/ │   │  │    │Connect/ │   │       │
│  │    │Disconnect│  │  │    │Disconnect│  │       │
│  │    │Callbacks│   │  │    │Callbacks│   │       │
│  │    └─────────┘   │  │    └─────────┘   │       │
│  └─────────────────┘  └─────────────────┘       │
└─────────────────────────────────────────────────┘
```

## License

MIT — see [LICENSE](LICENSE).

This library is derived from [libcluster](https://github.com/bitwalker/libcluster) by Paul Schoenfelder, used under the MIT License.
