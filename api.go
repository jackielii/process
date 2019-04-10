// Package process is a distributed task runner
// It uses machinery under the hood to provide distributed messenging
// It simulates the python's subprocess package and provide Call, Interrupt & GetResult
package process

import (
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jackielii/machinery/v1"
	"github.com/jackielii/machinery/v1/backends/result"
	"github.com/jackielii/machinery/v1/config"
	"github.com/jackielii/machinery/v1/tasks"
	"github.com/pkg/errors"
)

// Process is the process's base struct, use New to create a new instance
type Process struct {
	server   *machinery.Server
	errChan  chan error
	worker   *machinery.Worker
	jobQuery *JobQuery

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
	jobQuery, err := NewJobQuery(redisDSN)
	if err != nil {
		return nil, errors.Wrap(err, "init new process")
	}

	p := &Process{
		server:   server,
		worker:   worker,
		errChan:  errChan,
		jobQuery: jobQuery,
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
	case <-time.Tick(d):
		return errors.New("timeout exceeded")
	}
}

// Register registers a function as a runnable function in the process
func (p Process) Register(funcName string, function interface{}) error {
	err := p.server.RegisterTask(funcName, function)
	if err != nil {
		return errors.Wrap(err, "register process")
	}
	return nil
}

// Invoke registers the func with it's reflect name, and sends the task
func (p Process) Invoke(f interface{}, args []tasks.Arg) (jobID string, err error) {
	if reflect.TypeOf(f).Kind() != reflect.Func {
		return "", errors.New("f is not a function")
	}
	funcName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	println(funcName)
	registered := p.server.IsTaskRegistered(funcName)
	if !registered {
		err = p.server.RegisterTask(funcName, f)
		if err != nil {
			return "", err
		}
	}

	return p.Call(funcName, args)
}

// Call calls a registered function, the arguments needs to be in the machinery []Arg format
func (p Process) Call(funcName string, args []tasks.Arg) (jobID string, err error) {
	sig, err := tasks.NewSignature(funcName, args)
	if err != nil {
		return "", errors.Wrap(err, "process call")
	}

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

// GetJobQuery is a helper that returns a job query
func (p Process) GetJobQuery() *JobQuery {
	return p.jobQuery
}

// JobQuery is a redis conn with lock
type JobQuery struct {
	redisConn redis.Conn // TODO: use redis.Pool
	redisLock *sync.Mutex
	done      chan struct{}
}

// NewJobQuery returns a new job query
func NewJobQuery(redisDSN string) (*JobQuery, error) {
	host := strings.Replace(redisDSN, "redis://", "", -1)
	redisConn, err := redis.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	return &JobQuery{
		redisConn: redisConn,
		redisLock: &sync.Mutex{},
		done:      done,
	}, nil
}

// Close cleans up the goroutines if any
func (p JobQuery) Close() error {
	select {
	case p.done <- struct{}{}:
	default:
	}
	return p.redisConn.Close()
	// return nil
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
		println(err.Error())
		return false
	}
	return v != ""
}

// InterruptedChan will notify the interruptChan if the job is interrupted
func (p JobQuery) InterruptedChan(jobID string) <-chan struct{} {
	interruptedChan := make(chan struct{})
	go func() {
		// defer func() {
		//     fmt.Println("check interrupted exited")
		// }()
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

// ProgressChan returns a receive only channel, and progress send to this channel will be set
// Note that the error of the set progress is ignored
// send to done channel cleans it up
func (p JobQuery) ProgressChan(jobID string) chan<- string {
	// defer func() {
	//     fmt.Println("check progress exited")
	// }()
	ch := make(chan string)
	go func() {
		for {
			select {
			case progress := <-ch:
				p.SetProgress(jobID, progress)
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
	if err != nil && err != redis.ErrNil {
		return "", err
	}
	return s, nil
}

func progressSubject(jobID string) string {
	return "progress_" + jobID
}

func interruptSubject(jobID string) string {
	return "interrupt_" + jobID
}
