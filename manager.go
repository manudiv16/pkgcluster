package pkgcluster

import (
	"context"
	"sync"
)

// managerInstance tracks a running strategy.
type managerInstance struct {
	topology Topology
	strategy Strategy
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// Manager runs a collection of topology-based discovery strategies.
// It is the equivalent of libcluster's Cluster.Supervisor but much lighter:
// it simply starts each strategy in its own goroutine and coordinates
// shutdown.
type Manager struct {
	mu   sync.Mutex
	inst []*managerInstance
}

// NewManager creates a Manager from a list of topologies.
// No strategies are started until Start is called.
func NewManager(topologies ...Topology) *Manager {
	inst := make([]*managerInstance, 0, len(topologies))
	for _, t := range topologies {
		inst = append(inst, &managerInstance{topology: t})
	}
	return &Manager{inst: inst}
}

// Start starts all topology strategies. Each strategy runs in its own
// goroutine and is cancelled when ctx is cancelled or Stop is called.
// If a strategy fails to initialise, it is logged but does not prevent
// other strategies from starting.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, inst := range m.inst {
		if inst.cancel != nil {
			continue // already started
		}

		strategy, err := NewStrategy(inst.topology.Strategy, inst.topology.initialState())
		if err != nil {
			globalLogger.Error("failed to create strategy %q for topology %q: %v",
				inst.topology.Strategy, inst.topology.Name, err)
			continue
		}

		runCtx, cancel := context.WithCancel(ctx)
		inst.strategy = strategy
		inst.cancel = cancel

		inst.wg.Add(1)
		go func(name string, s Strategy) {
			defer inst.wg.Done()
			globalLogger.Info("starting %s strategy for topology %q", s.Name(), name)
			if err := s.Run(runCtx, inst.topology.initialState()); err != nil {
				if err != context.Canceled {
					globalLogger.Error("%s strategy for topology %q exited: %v",
						s.Name(), name, err)
				}
			}
			globalLogger.Info("%s strategy for topology %q stopped", s.Name(), name)
		}(inst.topology.Name, strategy)
	}
}

// Stop cancels all running strategies and waits for them to finish.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, inst := range m.inst {
		if inst.cancel != nil {
			inst.cancel()
		}
	}
	for _, inst := range m.inst {
		inst.wg.Wait()
		inst.cancel = nil
		inst.strategy = nil
	}
}
