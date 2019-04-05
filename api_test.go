package process

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProcess(t *testing.T) {
	redisDSN := "redis://localhost:6379"
	p, err := New(redisDSN)
	require.NoError(t, err)
	err = p.Register("call", task)
	require.NoError(t, err)
	jobID, err := p.Call("call", []tasks.Arg{
		{
			Type:  "string",
			Value: "sending arg1",
		},
	})
	require.NoError(t, err)

	r := p.GetResult(jobID)
	t.Logf("jobID: %s", jobID)
	j := p.GetJobQuery()
	i := 0
	prevProgress := ""
	for {
		rs, err := r.Touch()
		require.NoError(t, err)
		if rs != nil {
			break
		}
		progress, err := j.CheckProgress(jobID)
		require.NoError(t, err)
		if progress != prevProgress {
			prevProgress = progress
			assert.Equal(t, strconv.Itoa(i), progress, "progress")
			i++
		}
		if i == 50 {
			break
		}
	}
	err = j.Interrupt(jobID)
	require.NoError(t, err)

	v, err := r.GetWithTimeout(3*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, "interrupted", v[0].String(), "message from task function")

	// test normal
	jobID, err = p.Call("call", []tasks.Arg{
		{
			Type:  "string",
			Value: "sending arg1",
		},
	})
	require.NoError(t, err)
	r = p.GetResult(jobID)
	v, err = r.GetWithTimeout(3*time.Second, 100*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, constMsg, v[0].String(), "message from task function")
	// p.Wait() // we should wait, commenting out just for testing
}

const constMsg = "message from task"

func task(ctx context.Context, msg string) (string, error) {
	// fmt.Println("received ", msg)

	sig := tasks.SignatureFromContext(ctx)
	if sig == nil {
		return "", errors.New("unable to task sigature")
	}
	jobID := sig.UUID
	p, err := NewJobQuery("redis://localhost:6379")
	if err != nil {
		return "", err
	}

	interruptedChan := make(chan struct{})

	go func() {
		for {
			interrupted := p.Interrupted(jobID)
			if interrupted {
				interruptedChan <- struct{}{}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	for i := 0; i < 100; i++ {
		select {
		case <-interruptedChan:
			return "interrupted", nil
		default:
			p.SetProgress(jobID, strconv.Itoa(i))
			time.Sleep(10 * time.Millisecond)
		}
	}
	// spew.Dump("context within the task: %v", ctx)
	return constMsg, nil
}
