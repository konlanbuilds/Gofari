package executor

import "fmt"

type Registry struct {
	registry map[string]Executor
}

func NewRegistry() *Registry {
	return &Registry{
		registry: make(map[string]Executor),
	}
}

func (r *Registry) Register(name string, e Executor) error {
	if name == "" {
		return fmt.Errorf("executor name required")
	}
	if e == nil {
		return fmt.Errorf("executor required")
	}
	if _, ok := r.registry[name]; ok {
		return fmt.Errorf("executor %q already registered", name)
	}
	r.registry[name] = e
	return nil
}

func (r *Registry) Resolve(name string) (Executor, bool) {
	if r == nil {
		return nil, false
	}
	e, ok := r.registry[name]
	return e, ok
}

func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.registry))
	for k := range r.registry {
		names = append(names, k)
	}
	return names
}
