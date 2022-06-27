package task

import (
	"context"
	"time"
)

const (
	ModeCron   = "Cron"
	ModePeriod = "Period"

	DefaultCronExtendedTaskId = "CronExtendedTask"
	DefaultCronSpec           = "* * * * *"

	DefaultPeriodExtendedTaskId = "PeriodExtendedTask"
	DefaultPeriodInterval       = time.Minute
	DefaultPeriodOffset         = 0
	DefaultPeriodAlign          = true
	DefaultPeriodStrictAlign    = false
)

type Task interface {
	GetTaskId() string
	GetContext() context.Context
	GetMode() string
	Cancel()
	IsCanceled() bool
	OnError(error)
	GetLastError() error
}

type AbstractTask interface {
	Task

	Run() error
}

type ExtendedTask interface {
	Task

	PreExecute() bool
	Execute() bool
	CleanUp() error
}

type CronTask interface {
	Task

	GetCronSpec() string
}

type PeriodTask interface {
	Task

	GetInterval() time.Duration
	GetOffset() time.Duration
	GetAlign() bool
	GetStrictAlign() bool
	GetRescheduleSignal() chan struct{}
	GetNextExecuteTime() time.Time

	SetNextExecuteTime(time.Time)
}

type CronExtendedTask interface {
	CronTask
	ExtendedTask
}

type PeriodExtendedTask interface {
	PeriodTask
	ExtendedTask
}

type DefaultTask struct {
	id     string
	mode   string
	ctx    context.Context
	cancel context.CancelFunc

	lastErr error
}

func NewDefaultTask(id string) *DefaultTask {
	ctx, cancel := context.WithCancel(context.Background())
	return &DefaultTask{
		id:     id,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (task *DefaultTask) WithMode(mode string) *DefaultTask {
	task.mode = mode
	return task
}

func (task *DefaultTask) WithCronMode() *DefaultTask {
	return task.WithMode(ModeCron)
}

func (task *DefaultTask) WithPeriodMode() *DefaultTask {
	return task.WithMode(ModePeriod)
}

func (task *DefaultTask) GetTaskId() string {
	return task.id
}

func (task *DefaultTask) GetMode() string {
	return task.mode
}

func (task *DefaultTask) GetContext() context.Context {
	return task.ctx
}

func (task *DefaultTask) Cancel() {
	task.cancel()
}

func (task *DefaultTask) IsCanceled() bool {
	select {
	case <-task.ctx.Done():
		return true
	default:
		return false
	}
}

func (task *DefaultTask) Sleep(interval time.Duration) bool {
	select {
	case <-task.GetContext().Done():
		return false
	case <-time.After(interval):
		return true
	}
}

func (task *DefaultTask) OnError(err error) {
	task.lastErr = err
}

func (task *DefaultTask) GetLastError() error {
	return task.lastErr
}

type DefaultCronTask struct {
	Task

	cronSpec string
}

func NewDefaultCronTask(id, cronSpec string) *DefaultCronTask {
	if id == "" {
		id = DefaultCronExtendedTaskId
	}
	if cronSpec == "" {
		cronSpec = DefaultCronSpec
	}
	return &DefaultCronTask{
		Task:     NewDefaultTask(id).WithCronMode(),
		cronSpec: cronSpec,
	}
}

func (cronTask *DefaultCronTask) WithTask(task Task) *DefaultCronTask {
	cronTask.Task = task
	return cronTask
}

func (cronTask *DefaultCronTask) GetCronSpec() string {
	return cronTask.cronSpec
}

type DefaultPeriodTask struct {
	Task

	interval         time.Duration
	offset           time.Duration
	align            bool
	strictAlign      bool
	rescheduleSignal chan struct{}

	nextExecuteTime time.Time
}

func NewDefaultPeriodTask(id string, interval time.Duration) *DefaultPeriodTask {
	if id == "" {
		id = DefaultPeriodExtendedTaskId
	}
	if interval == 0 {
		interval = DefaultPeriodInterval
	}
	return &DefaultPeriodTask{
		Task:             NewDefaultTask(id).WithPeriodMode(),
		interval:         interval,
		rescheduleSignal: make(chan struct{}),
		offset:           DefaultPeriodOffset,
		align:            DefaultPeriodAlign,
		strictAlign:      DefaultPeriodStrictAlign,
	}
}

func (PeriodTask *DefaultPeriodTask) WithTask(task Task) *DefaultPeriodTask {
	PeriodTask.Task = task
	return PeriodTask
}

func (PeriodTask *DefaultPeriodTask) GetInterval() time.Duration {
	return PeriodTask.interval
}

func (PeriodTask *DefaultPeriodTask) GetOffset() time.Duration {
	return PeriodTask.offset
}

func (PeriodTask *DefaultPeriodTask) GetAlign() bool {
	return PeriodTask.align
}

func (PeriodTask *DefaultPeriodTask) GetStrictAlign() bool {
	return PeriodTask.strictAlign
}

func (PeriodTask *DefaultPeriodTask) GetRescheduleSignal() chan struct{} {
	return PeriodTask.rescheduleSignal
}

func (PeriodTask *DefaultPeriodTask) GetNextExecuteTime() time.Time {
	return PeriodTask.nextExecuteTime
}

func (PeriodTask *DefaultPeriodTask) SetNextExecuteTime(nextTime time.Time) {
	PeriodTask.nextExecuteTime = nextTime
}
