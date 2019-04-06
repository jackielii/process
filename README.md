# process

[![](https://godoc.org/github.com/jackielii/process?status.svg)](https://godoc.org/github.com/jackielii/process)
[![Build Status](https://travis-ci.com/jackielii/process.svg?branch=master)](https://travis-ci.com/jackielii/process)

process adds interruptable & setting-progress capability to [machinery](https://github.com/RichardKnop/machinery/) lib.

Until [this PR](https://github.com/RichardKnop/machinery/pull/401) is merged, we have to use my fork

Example usage: see [api_test.go](https://github.com/jackielii/process/blob/master/api_test.go)

```go

redisDSN := "redis://localhost:6379"

task := func(ctx context.Context, msg string) (string, error) {
	sig := tasks.SignatureFromContext(ctx)
	if sig == nil {
		return "", errors.New("unable to retrieve task signature")
	}
	jobID := sig.UUID
	j, err := NewJobQuery(redisDSN)
	if err != nil {
		return "", err
	}
	defer j.Close()

	interruptedChan := j.InterruptedChan(jobID)
	processChan := j.ProgressChan(jobID)

	// emulate a long running task
	for i := 0; i < 100; i++ {
		select {
		case <-interruptedChan:
			return "interrupted", nil
		case processChan <- strconv.Itoa(i):
			time.Sleep(10 * time.Millisecond)
		}
	}
	return "received " + msg, nil
}

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
j := p.GetJobQuery()
i := 0
prevProgress := ""
for {
	rs, err := r.Touch()
	require.NoError(t, err)
	if rs != nil {
		break
	}
	progress, err := j.GetProgress(jobID)
	require.NoError(t, err)
	if progress != prevProgress {
		prevProgress = progress
		pi, _ := strconv.Atoi(progress)
		// because the actual progress will run faster than the checking
		// if synchronized behaviour is expected, check the above example
		assert.True(t, i < pi, "progress")
		i++
	}
	if i >= 10 {
		err = j.Interrupt(jobID)
		require.NoError(t, err)
	}
}

v, err := r.Get(10 * time.Millisecond)
require.NoError(t, err)
assert.Equal(t, "interrupted", v[0].String(), "message from task function")
```
