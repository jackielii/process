// Package process is a distributed task runner
// It uses machinery under the hood to provide distributed messenging
// It simulates the python's subprocess package and provide Call, Interrupt & GetResult
package process

import (
	"context"
	"encoding/json"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

var ErrUnknowJobID = errors.New("unknow job id")

// Process is the process's base struct, use New to create a new instance
type Process struct {
	server   *machinery.Server
	errChan  chan error
	worker   *machinery.Worker
	redisDSN string

	closed bool
}

// New create a new process package, convention similiar to python's subprocess package
func New(redisDSN string) (*Process, error) {
	cfg := config.Config{
		Broker:        redisDSN,
		ResultBackend: redisDSN,
	}
	server, err := machinery.NewServer(&cfg)
	if err != nil {
		return nil, errors.Wrap(err, "init new process")
	}
	errChan := make(chan error)
	worker := server.NewWorker("worker0", runtime.NumCPU())
	worker.LaunchAsync(errChan)

	redisConn, err := redis.Dial("tcp", strings.Replace(redisDSN, "redis://", "", -1))
	if err != nil {
		return nil, errors.Wrap(err, "init new process")
	}
	_, err = redisConn.Do("PING")
	if err != nil {
		return nil, errors.Wrap(err, "init new process")
	}

	p := &Process{
		server:   server,
		worker:   worker,
		errChan:  errChan,
		redisDSN: redisDSN,
	}
	server.SetPreTaskHandler(p.prePublish)

	runtime.SetFinalizer(p, (*Process).Wait)

	return p, nil
}

func (p Process) prePublish(sig *tasks.Signature) {
}

// Wait waits for the process to finish
// unless Quit() is called or Interrupt signal is send, the process won't exit
func (p *Process) Wait() error {
	return p.WaitFor(1 * time.Minute)
}

// WaitFor waits for duration before exit
func (p *Process) WaitFor(d time.Duration) error {
	if p.closed {
		return nil
	}
	select {
	case err := <-p.errChan:
		runtime.SetFinalizer(p, nil)
		p.closed = true
		return err
	case <-time.After(d):
		return errors.New("timeout exceeded")
	}
}

// RegisterFunc registers the function using it's reflection name
func (p Process) RegisterFunc(function interface{}) (funcName string, err error) {
	funcName, err = fn(function)
	if err != nil {
		return "", err
	}
	registered := p.server.IsTaskRegistered(funcName)
	if !registered {
		err = p.server.RegisterTask(funcName, function)
		if err != nil {
			return funcName, err
		}
	}
	return funcName, nil
}

// Register registers a function as a runnable function in the process
func (p Process) Register(funcName string, function interface{}) error {
	err := p.server.RegisterTask(funcName, function)
	if err != nil {
		return errors.Wrap(err, "register process")
	}
	return nil
}

// Invoke calls the func by its reflect name
func (p Process) Invoke(f interface{}, args []tasks.Arg) (jobID string, err error) {
	return p.InvokeWithHeaders(f, args, nil)
}

// InvokeWithHeaders calls the function by its reflect name with headers
func (p Process) InvokeWithHeaders(f interface{}, args []tasks.Arg, headers tasks.Headers) (jobID string, err error) {
	funcName, err := fn(f)
	if err != nil {
		return "", err
	}
	return p.CallWithHeaders(funcName, args, headers)
}

// Call calls a registered function, the arguments needs to be in the machinery []Arg format
func (p Process) Call(funcName string, args []tasks.Arg) (jobID string, err error) {
	return p.CallWithHeaders(funcName, args, nil)
}

// CallWithHeaders calls a register function with metadata
func (p Process) CallWithHeaders(funcName string, args []tasks.Arg, headers tasks.Headers) (jobID string, err error) {
	if !p.server.IsTaskRegistered(funcName) {
		return "", errors.Errorf("function %s is not registered", funcName)
	}

	sig, err := tasks.NewSignature(funcName, args)
	if err != nil {
		return "", errors.Wrap(err, "process call")
	}

	sig.Headers = headers

	r, err := p.server.SendTask(sig)
	if err != nil {
		return "", errors.Wrapf(err, "call func %s", funcName)
	}

	return r.Signature.UUID, nil
}

// GetResult retrives a AsyncResult using the jobID
func (p Process) GetResult(jobID string) *result.AsyncResult {
	return result.NewAsyncResult(&tasks.Signature{UUID: jobID}, p.server.GetBackend())
}

// OpenJobQuery is a helper that returns a new job query
func (p Process) OpenJobQuery() (*JobQuery, error) {
	return OpenJobQuery(p.redisDSN)
}

// Interrupt sends interrupt signal
func (p Process) Interrupt(jobID string) error {
	s := p.GetResult(jobID).GetState()

	if s.TaskUUID != jobID {
		return errors.New("unknow job")
	}
	j, err := p.OpenJobQuery()
	if err != nil {
		return err
	}
	defer j.Close()

	return j.Interrupt(jobID)
}

// GetProgress retrieves the progress
func (p Process) GetProgress(jobID string) (string, error) {
	s := p.GetResult(jobID).GetState()

	if s.TaskUUID != jobID {
		return "", errors.New("unknow job")
	}
	j, err := p.OpenJobQuery()
	if err != nil {
		return "", err
	}
	defer j.Close()

	return j.GetProgress(jobID)
}

// JobQuery is a redis conn with lock, remember to close after open
type JobQuery struct {
	redisConn redis.Conn
	redisLock *sync.Mutex
	wg        *sync.WaitGroup
	done      chan struct{}
}

// OpenJobQuery returns a new job query
func OpenJobQuery(redisDSN string) (*JobQuery, error) {
	host := strings.Replace(redisDSN, "redis://", "", -1)
	redisConn, err := redis.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	return NewJobQuery(redisConn), nil
}

// NewJobQuery creates a new job query
func NewJobQuery(redisConn redis.Conn) *JobQuery {
	done := make(chan struct{})
	return &JobQuery{
		redisConn: redisConn,
		redisLock: &sync.Mutex{},
		wg:        &sync.WaitGroup{},
		done:      done,
	}
}

// Close cleans up the goroutines if any
func (p JobQuery) Close() error {
	close(p.done)
	p.wg.Wait()
	return p.redisConn.Close()
}

// Interrupt sends a interrupt signal to the running job.
// If the job implements subscribes to the job event, it should exit
func (p JobQuery) Interrupt(jobID string) error {
	c := p.redisConn

	p.redisLock.Lock()
	defer p.redisLock.Unlock()

	err := c.Send("SET", interruptSubject(jobID), "interrupt")
	if err != nil {
		return err
	}
	err = c.Send("EXPIRE", interruptSubject(jobID), 60*60) // expires in 1 hour
	if err != nil {
		return err
	}
	err = c.Flush()
	if err != nil {
		return err
	}

	return err
}

// Interrupted checks if the job is interrupted synchronously
func (p JobQuery) Interrupted(jobID string) bool {
	c := p.redisConn

	p.redisLock.Lock()
	defer p.redisLock.Unlock()

	v, err := redis.String(c.Do("GET", interruptSubject(jobID)))
	if err != nil && err != redis.ErrNil {
		return false
	}
	return v != ""
}

// WithInterruptCtx will cancel context if interrupted
func (p JobQuery) WithInterruptCtx(ctx context.Context, jobID string) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer cancel()
		// defer func() {
		//     fmt.Println("check interrupted exited")
		// }()
		for {
			interrupted := p.Interrupted(jobID)
			if interrupted {
				cancel()
				return
			}
			time.Sleep(10 * time.Millisecond)

			select {
			case <-p.done:
				return
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return ctx
}

// InterruptedChan will notify the interruptChan if the job is interrupted
func (p JobQuery) InterruptedChan(jobID string) <-chan struct{} {
	interruptedChan := make(chan struct{})
	p.wg.Add(1)
	go func() {
		// defer func() {
		//     fmt.Println("check interrupted exited")
		// }()
		defer p.wg.Done()
		for {
			interrupted := p.Interrupted(jobID)
			if interrupted {
				interruptedChan <- struct{}{}
				return
			}
			time.Sleep(10 * time.Millisecond)

			select {
			case <-p.done:
				return
			default:
			}
		}
	}()
	return interruptedChan
}

// SetProgress sets the progress for a job
// progress will expire in 1 minute
func (p JobQuery) SetProgress(jobID string, progress string) error {
	c := p.redisConn

	p.redisLock.Lock()
	defer p.redisLock.Unlock()

	err := c.Send("SET", progressSubject(jobID), progress)
	if err != nil {
		return err
	}
	err = c.Send("EXPIRE", progressSubject(jobID), 60) // expires in one minute
	if err != nil {
		return err
	}
	err = c.Flush()
	if err != nil {
		return err
	}

	return nil
}

// // WithCtx returns a new context with interrupt & progress injected
// func (p JobQuery) WithCtx(ctx context.Context, jobID string) context.Context {
//     ctx = p.WithInterruptCtx(ctx, jobID)
//     return p.WithProgressChanCtx(ctx, jobID)
// }
//
// type progressCtxType struct{}
//
// var progressCtxKey progressCtxType
//
// // ProgressChanFromCtx returns the progress receive only channel if in the context
// func ProgressChanFromCtx(ctx context.Context) chan<- string {
//     v := ctx.Value(progressCtxKey)
//     if v == nil {
//         return nil
//     }
//     return v.(chan<- string)
// }
//
// // WithProgressChanCtx returns new context with progress channel injected
// func (p JobQuery) WithProgressChanCtx(ctx context.Context, jobID string) context.Context {
//     ch := p.ProgressChan(jobID)
//     return context.WithValue(ctx, progressCtxKey, ch)
// }

// ProgressChanNoWait returns a receive only channel, and progress send to this channel will be set
// Note that the error of the set progress is ignored
// send to done channel cleans it up
// the difference between this and ProgressChan is this will not wait for SetProgress to finish
// in effect, it only sets the latest progress
func (p JobQuery) ProgressChanNoWait(jobID string) chan<- string {
	// defer func() {
	//     fmt.Println("check progress exited")
	// }()
	pch := make(chan string)    // receives all progress
	lpch := make(chan string)   // receives only latest progress
	done := make(chan struct{}) // signal no more progress is here

	p.wg.Add(1)
	// go routine waiting for the latest progress
	go func() {
		defer p.wg.Done()
		for {
			select {
			case progress := <-lpch:
				if progress == "" {
					continue
				}
				// println("setting progress", jobID, progress)
				err := p.SetProgress(jobID, progress)
				if err != nil {
					log.WARNING.Printf("failed to set progress for job id %s: %s, err: %v", jobID, progress, err)
				}
			case <-done:
				return
			}
		}
	}()

	p.wg.Add(1)
	// go routine sends only latest progress
	go func() {
		defer p.wg.Done()

		var sentProgress, progress string
		for {
			select {
			case lpch <- progress:
				sentProgress = progress
			case <-p.done:
				// make sure the last message is sent
				if sentProgress != progress {
					lpch <- progress
				}
				close(done)
				return
			default:
				select {
				case progress = <-pch:
				case <-p.done:
					close(done)
					return
				}
			}
		}
	}()
	return pch
}

// ProgressChan returns a receive only channel, and progress send to this channel will be set
// Note that the error of the set progress is ignored
// send to done channel cleans it up
func (p JobQuery) ProgressChan(jobID string) chan<- string {
	// defer func() {
	//     fmt.Println("check progress exited")
	// }()
	ch := make(chan string)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case progress := <-ch:
				err := p.SetProgress(jobID, progress)
				if err != nil {
					log.WARNING.Printf("failed to set progress for job id %s: %s, err: %v", jobID, progress, err)
				}
			case <-p.done:
				return
			}
		}
	}()
	return ch
}

// GetProgress returns the progress if the job implements progress
func (p JobQuery) GetProgress(jobID string) (progress string, err error) {
	c := p.redisConn

	p.redisLock.Lock()
	defer p.redisLock.Unlock()

	s, err := redis.String(c.Do("GET", progressSubject(jobID)))
	if err == redis.ErrNil {
		return "", ErrUnknowJobID
	}
	if err != nil {
		return "", err
	}
	return s, nil
}

// AddHeaders adds headers to the job store
func (p JobQuery) AddHeaders(jobID string, headers map[string]interface{}) error {
	for key, value := range headers {
		err := p.AddHeader(jobID, key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddHeader persists data into redis so that it can be retrieved by GetHeader
func (p JobQuery) AddHeader(jobID string, key string, value interface{}) (err error) {
	c := p.redisConn

	p.redisLock.Lock()
	defer p.redisLock.Unlock()

	// TODO: expire should be automatic set with jobQuery,
	// consider add expire on get
	expire := 24 * time.Hour

	headers := make(map[string]interface{})

	hs := headerSubject(jobID)
	data, err := redis.Bytes(c.Do("GET", hs))
	if err != nil && err != redis.ErrNil {
		return errors.Wrap(err, "add header")
	}
	if data != nil {
		err = json.Unmarshal(data, &headers)
		if err != nil {
			return errors.Wrap(err, "add header")
		}
	}

	headers[key] = value
	data, err = json.Marshal(headers)
	if err != nil {
		return errors.Wrap(err, "add header")
	}

	err = c.Send("SET", hs, data)
	if err != nil {
		return errors.Wrap(err, "add header")
	}
	err = c.Send("EXPIRE", hs, int(expire/time.Second))
	if err != nil {
		return errors.Wrap(err, "add header")
	}
	err = c.Flush()
	if err != nil {
		return errors.Wrap(err, "add header")
	}
	return nil
}

// GetHeader gets the header
func (p JobQuery) GetHeader(jobID, key string) (interface{}, error) {
	headers := make(map[string]interface{})
	err := p.UnmarshalHeaders(jobID, &headers)
	if err != nil {
		return nil, errors.Wrap(err, "get header")
	}
	header, ok := headers[key]
	if !ok {
		return nil, errors.New("key doesn't exist")
	}

	return header, nil
}

// UnmarshalHeader unmarshal the header with key
func (p JobQuery) UnmarshalHeader(jobID, key string, v interface{}) error {
	headers := make(map[string]json.RawMessage)
	err := p.UnmarshalHeaders(jobID, &headers)
	if err != nil {
		return err
	}

	header, ok := headers[key]
	if !ok {
		return errors.New("key doesn't exist")
	}

	return errors.Wrap(json.Unmarshal(header, v), "unmarshal header")
}

// UnmarshalHeaders unmarshals the headers
func (p JobQuery) UnmarshalHeaders(jobID string, v interface{}) error {
	c := p.redisConn

	p.redisLock.Lock()
	defer p.redisLock.Unlock()

	data, err := redis.Bytes(c.Do("GET", headerSubject(jobID)))
	if err != nil {
		return errors.Wrap(err, "get header")
	}

	err = json.Unmarshal(data, &v)
	if err != nil {
		return errors.Wrap(err, "get header")
	}
	return nil
}

func progressSubject(jobID string) string {
	return "progress_" + jobID
}

func interruptSubject(jobID string) string {
	return "interrupt_" + jobID
}

func headerSubject(jobID string) string {
	return "headers_" + jobID
}

func fn(function interface{}) (string, error) {
	if reflect.TypeOf(function).Kind() != reflect.Func {
		return "", errors.New("f is not a function")
	}
	return runtime.FuncForPC(reflect.ValueOf(function).Pointer()).Name(), nil
}
