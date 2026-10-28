package executor

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/konlanbuilds/Gofari/jobs"
)

type Executor interface {
	Execute(ctx context.Context, job jobs.Job) (json.RawMessage, error)
}

type TestSuccess struct{}

func (t *TestSuccess) Execute(ctx context.Context, job jobs.Job) (json.RawMessage, error) {
	return json.RawMessage(`{"ok":true}`), nil
}

type TestFail struct{}

func (t *TestFail) Execute(ctx context.Context, job jobs.Job) (json.RawMessage, error) {
	return nil, errors.New("intentional failure")
}
