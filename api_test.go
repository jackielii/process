package process

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/jackielii/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProcess(t *testing.T) {
	redisDSN := "redis://localhost:6379"
	p, err := New(redisDSN)
	require.NoError(t, err)
	err = p.Register("callNew", task)
	require.NoError(t, err)
	jobID, err := p.Call("callNew", []tasks.Arg{
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
	jobID, err = p.Call("callNew", []tasks.Arg{
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
		return "", errors.New("unable to task signature")
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

func ExampleProcess() {
	redisDSN := "redis://localhost:6379"

	task := func(ctx context.Context, msg string) (string, error) {
		sig := tasks.SignatureFromContext(ctx)
		if sig == nil {
			return "", errors.New("unable to retrieve task signature")
		}
		jobID := sig.UUID
		p, err := NewJobQuery(redisDSN)
		if err != nil {
			return "", err
		}
		defer p.Close()

		interruptedChan := make(chan struct{})
		println(interruptedChan)
		done := make(chan struct{})

		go func() {
			for {
				interrupted := p.Interrupted(jobID)
				if interrupted {
					interruptedChan <- struct{}{}
					return
				}
				time.Sleep(10 * time.Millisecond)

				select {
				case <-done:
					return
				default:
				}
			}
		}()

		// emulate a long running task
		for i := 0; i < 100; i++ {
			p.SetProgress(jobID, strconv.Itoa(i))
			time.Sleep(10 * time.Millisecond)
			select {
			case <-interruptedChan:
				return "interrupted", nil
			default:
			}
		}
		done <- struct{}{}
		return "received " + msg, nil
	}

	// main goroutine
	p, err := New(redisDSN)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Register("callExample", task)
	if err != nil {
		log.Fatal(err)
	}
	jobID, err := p.Call("callExample", []tasks.Arg{
		{
			Type:  "string",
			Value: "hello from machinery",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	r := p.GetResult(jobID)
	j := p.GetJobQuery()
	i := 0
	prevProgress := ""
	for {
		rs, err := r.Touch()
		if err != nil {
			log.Fatal(err)
		}
		if rs != nil {
			break
		}
		progress, err := j.GetProgress(jobID)
		if err != nil {
			log.Fatal(err)
		}
		if progress != prevProgress {
			prevProgress = progress
			fmt.Println(progress)
			i++
		}
		// simulate a interrupt
		if i == 10 {
			err = j.Interrupt(jobID)
			if err != nil {
				log.Fatal(err)
			}

			break
		}
	}
	v, err := r.GetWithTimeout(3*time.Second, 100*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(v[0].String())
	// Output:
	// 0
	// 1
	// 2
	// 3
	// 4
	// 5
	// 6
	// 7
	// 8
	// 9
	// interrupted
}

func TestChannelAPI(t *testing.T) {
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
		println(interruptedChan)
		processChan := j.ProgressChan(jobID)

		// emulate a long running task
		for i := 0; i < 100; i++ {
			select {
			case <-interruptedChan:
				return "interrupted", nil
			case processChan <- strconv.Itoa(i):
			// case <-localInterrupt:
			//     time.Sleep(100 * time.Millisecond) // enough sleep to make sure the interrupt channel is ready
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
		return "received " + msg, nil
	}

	p, err := New(redisDSN)
	require.NoError(t, err)
	err = p.Register("callChannel", task)
	require.NoError(t, err)
	jobID, err := p.Call("callChannel", []tasks.Arg{
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
			assert.Truef(t, i < pi, "progress: %v, got: %v", i, pi)
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
}

func TestNonBlockingClose(t *testing.T) {
	j, err := NewJobQuery("redis://localhost:6379")
	require.NoError(t, err)
	j.Close()
}

func TestGracefulWait(t *testing.T) {
	p, err := New("redis://localhost:6379")
	require.NoError(t, err)
	syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	err = p.Wait()
	require.NoError(t, err)
}

func TestInvoke(t *testing.T) {
	p, err := New("redis://localhost:6379")
	require.NoError(t, err)
	task := func(msg string) (string, error) {
		return "received " + msg, nil
	}

	jobID, err := p.Invoke(task, []tasks.Arg{
		{
			Type:  "string",
			Value: "test invoke",
		},
	})
	require.NoError(t, err)

	r := p.GetResult(jobID)
	v, err := r.Get(1 * time.Millisecond)
	require.NoError(t, err)

	require.Equal(t, 1, len(v), "length of the returned results")
	assert.Equal(t, "received test invoke", v[0].String())
}
