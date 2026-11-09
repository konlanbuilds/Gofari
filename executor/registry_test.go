package executor

import "testing"

func TestRegistryContracts(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("task", &TestSuccess{}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("task", &TestSuccess{}); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}
	if _, ok := r.Resolve("task"); !ok {
		t.Fatal("registered executor was not resolved")
	}
	if err := r.Register("", &TestSuccess{}); err == nil {
		t.Fatal("expected empty name to fail")
	}
}
