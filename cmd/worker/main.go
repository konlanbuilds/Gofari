package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/konlanbuilds/Gofari/executor"
	"github.com/konlanbuilds/Gofari/jobs"
	"github.com/konlanbuilds/Gofari/queue/memory"
	"github.com/konlanbuilds/Gofari/worker"
)

func logEvent(ts time.Time, workerID string, event string, jobID uuid.UUID, target string, status string, extra string) {
	if extra == "" {
		fmt.Printf("[%s] [worker=%s] [event=%s] [job=%s] [target=%s] [status=%s]\n",
			ts.Format("15:04:05.000"), workerID, event, jobID.String()[:8], target, status)
		return
	}
	fmt.Printf("[%s] [worker=%s] [event=%s] [job=%s] [target=%s] [status=%s] %s\n",
		ts.Format("15:04:05.000"), workerID, event, jobID.String()[:8], target, status, extra)
}

func main() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	store := memory.New()
	reg := executor.NewRegistry()
	if err := reg.Register("test_success", &executor.TestSuccess{}); err != nil {
		panic(err)
	}
	if err := reg.Register("test_fail", &executor.TestFail{}); err != nil {
		panic(err)
	}

	numWorkers := 4
	workers := make([]*worker.Worker, 0, numWorkers)
	for i := 0; i < numWorkers; i++ {
		wid := fmt.Sprintf("worker-%d", i+1)
		workers = append(workers, worker.New(store, reg, wid, 30*time.Second))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Enqueue jobs
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				target := "test_success"
				if r.Intn(3) == 0 {
					target = "test_fail"
				}
				j := jobs.Job{
					TargetKind:     jobs.TargetTask,
					TargetName:     target,
					Payload:        []byte(`{}`),
					PayloadVersion: 1,
					MaxAttempts:    1,
				}
				enq, err := store.Enqueue(context.Background(), j)
				if err != nil {
					logEvent(time.Now(), "producer", "enqueue_error", uuid.Nil, target, "error", fmt.Sprintf("err=%v", err))
					continue
				}
				logEvent(time.Now(), "producer", "enqueued", enq.ID, enq.TargetName, string(enq.Status), "")
			}
		}
	}()

	// Run multiple workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		w := workers[i]
		wid := fmt.Sprintf("worker-%d", i+1)
		go func(wid string, w *worker.Worker) {
			defer wg.Done()
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					processed, err := w.ProcessOne(context.Background())
					if err != nil {
						logEvent(time.Now(), wid, "process_error", uuid.Nil, "-", "error", fmt.Sprintf("err=%v", err))
						continue
					}
					if !processed {
						// idle, no log spam
						continue
					}
					// We don't have easy access to job details from ProcessOne return
					// but we can log a generic processed event
					logEvent(time.Now(), wid, "processed", uuid.Nil, "-", "ok", "")
				}
			}
		}(wid, w)
	}

	<-ctx.Done()
	wg.Wait()
}
