package scheduler

import (
	"context"
	"fmt"
	"schedule-test/lib/log"
	"schedule-test/task"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	DefaultSchedulerChanIdNumber = 1000
	DefaultAddHeapDuration       = time.Minute * 11

	ScheduleModeCron   = "Cron"
	ScheduleModePeriod = "Period"
	ScheduleModePlan   = "Plan"
	ScheduleModeNone   = "None"
)

var AllScheduleModes = []string{
	ScheduleModeCron,
	ScheduleModePeriod,
	ScheduleModePlan,
	ScheduleModeNone,
}

type BaseScheduler struct {
	worker *Worker
}

func NewBaseScheduler(worker *Worker) *BaseScheduler {
	return &BaseScheduler{
		worker: worker,
	}
}

type Scheduler struct {
	*zap.Logger

	cronScheduler          *CronScheduler
	periodScheduler        *PeriodScheduler
	planScheduler          *PlanScheduler
	findScheduleObjectById func(ctx context.Context, id string) (BaseScheduleObject, error)
}

func NewScheduler(findScheduleObjectById func(ctx context.Context, id string) (BaseScheduleObject, error)) *Scheduler {
	scheduler := &Scheduler{
		Logger:                 log.Logger.Get().Named("Scheduler"),
		findScheduleObjectById: findScheduleObjectById,
	}

	return scheduler
}

func (scheduler *Scheduler) WithCronSchedule(cronScheduler *CronScheduler) *Scheduler {
	scheduler.cronScheduler = cronScheduler
	return scheduler
}

func (scheduler *Scheduler) WithPeriodSchedule(periodScheduler *PeriodScheduler) *Scheduler {
	scheduler.periodScheduler = periodScheduler
	return scheduler
}

func (scheduler *Scheduler) WithPlanSchedule(planScheduler *PlanScheduler) *Scheduler {
	scheduler.planScheduler = planScheduler
	return scheduler
}

// Schedule 分配给两个不同调度器处理
func (scheduler *Scheduler) Schedule(id string, scheduleObject BaseScheduleObject) {
	if scheduleObject == nil {
		// 根据id获取可用调度对象
		newScheduleObject, err := scheduler.findScheduleObjectById(nil, id)
		if err != nil {
			scheduler.Logger.Warn("FindScheduleObjectByIdFailed",
				zap.Error(err),
			)
			return
		}
		scheduleObject = newScheduleObject
	}

	if scheduler.cronScheduler != nil {
		scheduler.CronSchedule(scheduleObject)
	}

	if scheduler.periodScheduler != nil {
		scheduler.PeriodSchedule(scheduleObject)
	}

	if scheduler.planScheduler != nil {
		if _, ok := scheduleObject.IsPlanMode(); ok {
			scheduler.PlanSchedule(scheduleObject)
		}
	}

}

// CronSchedule 分配给Cron调度器处理
func (scheduler *Scheduler) CronSchedule(scheduleObject BaseScheduleObject) {
	go scheduler.cronScheduler.Schedule(scheduleObject)
}

// PeriodSchedule 分配给Period调度器处理
func (scheduler *Scheduler) PeriodSchedule(scheduleObject BaseScheduleObject) {
	go scheduler.periodScheduler.Schedule(scheduleObject)
}

// PlanSchedule 分配给Plan调度器处理
func (scheduler *Scheduler) PlanSchedule(scheduleObject BaseScheduleObject) {
	go scheduler.planScheduler.Schedule(scheduleObject)
}

func (scheduler *Scheduler) Start() (err error) {
	err = scheduler.planScheduler.Start()
	return err
}

func (scheduler *Scheduler) Stop() (err error) {
	err = scheduler.planScheduler.Stop()
	err = scheduler.cronScheduler.StopRunner()
	err = scheduler.periodScheduler.StopRunner()

	return err
}

// **************************  cron类型调度器  **************************

type CronScheduler struct {
	*BaseScheduler
	*zap.Logger

	manager CronScheduleObjectManager
	*task.DefaultCronTask
	task.Runner

	lock      sync.Mutex
	stop      bool
	RunnerMap map[string]*CronSchedulerRunner
}

func NewCronScheduler(worker *Worker, manager CronScheduleObjectManager) *CronScheduler {
	return &CronScheduler{
		Logger:        log.Logger.Get().Named("CronScheduler"),
		RunnerMap:     make(map[string]*CronSchedulerRunner),
		BaseScheduler: NewBaseScheduler(worker),
		manager:       manager,
	}
}

func (scheduler *CronScheduler) StopRunner() (err error) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	scheduler.stop = true
	for _, runner := range scheduler.RunnerMap {
		err = runner.Stop()
	}
	return err
}

func (scheduler *CronScheduler) Schedule(cronScheduleObject BaseScheduleObject) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()
	if scheduler.stop {
		return
	}

	runner, ok := scheduler.RunnerMap[cronScheduleObject.GetId()]
	// 创建跟更新runner区分，还需判断该调度对象是否已删除
	if ok {
		if cronScheduleObject.IsDeleted() {
			scheduler.deleteRunner(cronScheduleObject, runner)
		} else {
			scheduler.updateRunner(cronScheduleObject, runner)
		}

	} else {
		if !cronScheduleObject.IsDeleted() {
			scheduler.createRunner(cronScheduleObject)
		}
	}
}

func (scheduler *CronScheduler) createRunner(scheduleObject BaseScheduleObject) {
	// 如果调度模式不是cron类型，或者调度时间不正确，则不需要新增runner
	cronScheduleObject, ok := scheduleObject.IsCronMode()
	if !ok || cronScheduleObject == nil || cronScheduleObject.GetCronSpec() == "" {
		return
	}
	runner, err := NewCronSchedulerRunnerActuator(scheduler.manager, cronScheduleObject, scheduler.worker)
	if err != nil {
		runner.Warn("CronSchedulerCreateRunnerFailed",
			zap.Error(err),
		)
		return
	}
	err = runner.Start()
	if err != nil {
		runner.Warn("CronSchedulerCreateRunnerStartFailed",
			zap.Error(err),
		)
		return
	}
	scheduler.RunnerMap[scheduleObject.GetId()] = runner
	scheduler.Logger.Info(fmt.Sprintf("[%s]CronSchedulerCreateRunnerSuccess", scheduleObject.GetName()))
}

// 停掉旧的runner，并创建新的runner
func (scheduler *CronScheduler) updateRunner(scheduleObject BaseScheduleObject, oldRunner *CronSchedulerRunner) {
	// 判断版本，如果从数据库查出来的调度对象的版本不大于runner的调度对象的版本，则说明runner是最新的，不需要做任何处理
	if scheduleObject.GetVersion() <= oldRunner.scheduleObject.GetVersion() {
		return
	}

	oldRunner.Cancel()

	// 如果调度模式不是cron类型，则不需要新增runner，只需要清除map中旧的runner
	cronScheduleObject, ok := scheduleObject.IsCronMode()
	if !ok {
		delete(scheduler.RunnerMap, scheduleObject.GetId())
		scheduler.Logger.Info(fmt.Sprintf("[%s]CronSchedulerUpdateRunnerDeleteSuccess", scheduleObject.GetName()))
		return
	}

	// 如果调度时间不正确，则跳过
	if cronScheduleObject == nil || cronScheduleObject.GetCronSpec() == "" {
		return
	}

	newRunner, err := NewCronSchedulerRunnerActuator(scheduler.manager, cronScheduleObject, scheduler.worker)
	if err != nil {
		newRunner.Warn("CronSchedulerUpdateRunnerFailed",
			zap.Error(err),
		)
		return
	}
	err = newRunner.Start()
	if err != nil {
		newRunner.Warn("CronSchedulerUpdateRunnerStartFailed",
			zap.Error(err),
		)
		return
	}
	scheduler.RunnerMap[scheduleObject.GetId()] = newRunner
}

// 停掉runner，并从map中删除
func (scheduler *CronScheduler) deleteRunner(scheduleObject BaseScheduleObject, oldRunner *CronSchedulerRunner) {
	// 判断版本，如果从数据库查出来的调度对象的版本小于runner的调度对象的版本，则说明runner是最新的，不需要做任何处理
	if scheduleObject.GetVersion() < oldRunner.scheduleObject.GetVersion() {
		return
	}
	oldRunner.Cancel()
	delete(scheduler.RunnerMap, scheduleObject.GetId())
	scheduler.Logger.Info(fmt.Sprintf("[%s]CronSchedulerDeleteRunnerDeleteSuccess", scheduleObject.GetName()))
}

type CronSchedulerRunner struct {
	*zap.Logger
	*task.DefaultCronTask
	task.Runner

	manager        CronScheduleObjectManager
	scheduleObject CronScheduleObject
	worker         *Worker
}

func NewCronSchedulerRunnerActuator(manager CronScheduleObjectManager, scheduleObject CronScheduleObject, worker *Worker) (*CronSchedulerRunner, error) {
	schedulerRunner := &CronSchedulerRunner{
		Logger:          log.Logger.Get().Named(fmt.Sprintf("%s_v%d", scheduleObject.GetName(), scheduleObject.GetVersion())),
		DefaultCronTask: task.NewDefaultCronTask(scheduleObject.GetName(), scheduleObject.GetCronSpec()),
		manager:         manager,
		scheduleObject:  scheduleObject,
		worker:          worker,
	}

	runner, err := task.NewCronRunner(schedulerRunner)
	if err != nil {
		schedulerRunner.Warn("NewCronRunnerFailed",
			zap.Error(err),
		)
		return nil, err
	}

	schedulerRunner.Runner = runner

	return schedulerRunner, nil
}

func (schedulerRunner *CronSchedulerRunner) PreExecute() bool {
	schedulerRunner.Info("CronSchedulerRunnerActuatorReady")

	return true
}

func (schedulerRunner *CronSchedulerRunner) Execute() bool {
	if err := schedulerRunner.worker.Work(schedulerRunner.scheduleObject); err != nil {
		schedulerRunner.Warn("CronSchedulerRunnerWorkFailed",
			zap.Error(err),
		)
	}
	return true
}

func (schedulerRunner *CronSchedulerRunner) CleanUp() {
}

// **************************  period类型调度器  **************************

type PeriodScheduler struct {
	*BaseScheduler
	*zap.Logger

	manager PeriodScheduleObjectManager

	lock      sync.Mutex
	stop      bool
	RunnerMap map[string]*PeriodSchedulerRunner
}

func NewPeriodScheduler(worker *Worker, manager PeriodScheduleObjectManager) *PeriodScheduler {
	return &PeriodScheduler{
		Logger:        log.Logger.Get().Named("PeriodScheduler"),
		RunnerMap:     make(map[string]*PeriodSchedulerRunner),
		BaseScheduler: NewBaseScheduler(worker),
		manager:       manager,
	}
}

func (scheduler *PeriodScheduler) StopRunner() (err error) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	scheduler.stop = true
	for _, runner := range scheduler.RunnerMap {
		err = runner.Stop()
	}
	return err
}

func (scheduler *PeriodScheduler) Schedule(periodScheduleObject BaseScheduleObject) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()
	if scheduler.stop {
		return
	}

	runner, ok := scheduler.RunnerMap[periodScheduleObject.GetId()]
	// 创建跟更新runner区分，还需判断该调度对象是否已删除
	if ok {
		if periodScheduleObject.IsDeleted() {
			scheduler.deleteRunner(periodScheduleObject, runner)
		} else {
			scheduler.updateRunner(periodScheduleObject, runner)
		}

	} else {
		if !periodScheduleObject.IsDeleted() {
			scheduler.createRunner(periodScheduleObject)
		}
	}
}

func (scheduler *PeriodScheduler) createRunner(scheduleObject BaseScheduleObject) {
	// 如果调度模式不是period类型，或者调度时间不正确，则不需要新增runner
	periodScheduleObject, ok := scheduleObject.IsPeriodMode()
	if !ok || periodScheduleObject == nil || periodScheduleObject.GetSchedulePeriod() == 0 {
		return
	}

	runner, err := NewPeriodSchedulerRunnerActuator(scheduler.manager, periodScheduleObject, scheduler.worker)
	if err != nil {
		runner.Warn("PeriodSchedulerCreateRunnerFailed",
			zap.Error(err),
		)
		return
	}
	err = runner.Start()
	if err != nil {
		runner.Warn("PeriodSchedulerCreateRunnerStartFailed",
			zap.Error(err),
		)
		return
	}
	scheduler.RunnerMap[scheduleObject.GetId()] = runner
	scheduler.Logger.Info(fmt.Sprintf("[%s]PeriodSchedulerCreateRunnerSuccess", scheduleObject.GetName()))
}

// 停掉旧的runner，并创建新的runner
func (scheduler *PeriodScheduler) updateRunner(scheduleObject BaseScheduleObject, oldRunner *PeriodSchedulerRunner) {
	// 判断版本，如果从数据库查出来的调度对象的版本不大于runner的调度对象的版本，则说明runner是最新的，不需要做任何处理
	if scheduleObject.GetVersion() <= oldRunner.scheduleObject.GetVersion() {
		return
	}

	oldRunner.Cancel()

	// 如果调度模式不是period类型，则不需要新增runner，只需要清除map中旧的runner
	periodScheduleObject, ok := scheduleObject.IsPeriodMode()
	if !ok {
		delete(scheduler.RunnerMap, scheduleObject.GetId())
		scheduler.Logger.Info(fmt.Sprintf(
			"[%s]PeriodSchedulerUpdateRunnerDeleteSuceess", scheduleObject.GetName()))
		return
	}

	// 如果调度时间不正确，直接跳过
	if periodScheduleObject == nil || periodScheduleObject.GetSchedulePeriod() == 0 {
		return
	}

	newRunner, err := NewPeriodSchedulerRunnerActuator(scheduler.manager, periodScheduleObject, scheduler.worker)
	if err != nil {
		newRunner.Warn("PeriodSchedulerUpdateRunnerFailed",
			zap.Error(err),
		)
		return
	}
	err = newRunner.Start()
	if err != nil {
		newRunner.Warn("PeriodSchedulerUpdateRunnerStartFailed",
			zap.Error(err),
		)
		return
	}
	scheduler.RunnerMap[scheduleObject.GetId()] = newRunner
	scheduler.Logger.Info(fmt.Sprintf("[%s]PeriodSchedulerUpdateRunnerSuceess", scheduleObject.GetName()))
}

// 停掉runner，并从map中删除
func (scheduler *PeriodScheduler) deleteRunner(scheduleObject BaseScheduleObject, oldRunner *PeriodSchedulerRunner) {
	// 判断版本，如果从数据库查出来的调度对象的版本小于runner的调度对象的版本，则说明runner是最新的，不需要做任何处理
	if scheduleObject.GetVersion() < oldRunner.scheduleObject.GetVersion() {
		return
	}
	oldRunner.Cancel()
	delete(scheduler.RunnerMap, scheduleObject.GetId())
	scheduler.Logger.Info(fmt.Sprintf("[%s]PeriodSchedulerDeleteRunnerSuccess", scheduleObject.GetName()))
}

type PeriodSchedulerRunner struct {
	*zap.Logger

	*task.DefaultPeriodTask
	task.Runner

	manager        PeriodScheduleObjectManager
	scheduleObject PeriodScheduleObject
	worker         *Worker
}

func NewPeriodSchedulerRunnerActuator(manager PeriodScheduleObjectManager, scheduleObject PeriodScheduleObject, worker *Worker) (*PeriodSchedulerRunner, error) {
	schedulerRunner := &PeriodSchedulerRunner{
		Logger:            log.Logger.Get().Named(scheduleObject.GetName()),
		DefaultPeriodTask: task.NewDefaultPeriodTask(fmt.Sprintf("%s_v%d", scheduleObject.GetName(), scheduleObject.GetVersion()), scheduleObject.GetSchedulePeriod()),
		manager:           manager,
		scheduleObject:    scheduleObject,
		worker:            worker,
	}
	runner, err := task.NewPeriodRunner(schedulerRunner)
	if err != nil {
		schedulerRunner.Warn("NewPeriodRunnerFailed",
			zap.Error(err),
		)
		return nil, err
	}

	schedulerRunner.Runner = runner

	return schedulerRunner, nil
}

func (schedulerRunner *PeriodSchedulerRunner) PreExecute() bool {
	schedulerRunner.Info("PeriodSchedulerRunnerActuatorReady")

	return true
}

func (schedulerRunner *PeriodSchedulerRunner) Execute() bool {
	if err := schedulerRunner.worker.Work(schedulerRunner.scheduleObject); err != nil {
		schedulerRunner.Warn("PeriodSchedulerRunnerWorkFailed",
			zap.Error(err),
		)
	}
	return true
}

func (schedulerRunner *PeriodSchedulerRunner) CleanUp() {
}

// **************************  plan类型调度器  **************************

type PlanScheduler struct {
	*BaseScheduler
	*zap.Logger

	manager PlanScheduleObjectManager
	*task.DefaultPeriodTask
	task.Runner

	pendingId          string
	ids                chan string
	scheduleObjectHeap PlanScheduleObjectHeapByTime
	// 入堆里的时间准则，等于当前时间+addHeapDuration
	addHeapDuration time.Duration
}

func NewPlanScheduler(chanSize int, addHeapDuration time.Duration, worker *Worker, manager PlanScheduleObjectManager) (*PlanScheduler, error) {
	if chanSize == 0 {
		chanSize = DefaultSchedulerChanIdNumber
	}

	if addHeapDuration == 0 {
		addHeapDuration = DefaultAddHeapDuration
	}
	scheduler := &PlanScheduler{
		ids:                make(chan string, chanSize),
		scheduleObjectHeap: make(PlanScheduleObjectHeapByTime, 0),
		DefaultPeriodTask:  task.NewDefaultPeriodTask("PlanScheduler", time.Second),
		BaseScheduler:      NewBaseScheduler(worker),
		manager:            manager,
		addHeapDuration:    addHeapDuration,
		Logger:             log.Logger.Get().Named("PlanScheduler"),
	}

	runner, _ := task.NewPeriodRunner(scheduler)

	scheduler.Runner = runner

	return scheduler, nil
}

func (scheduler *PlanScheduler) Schedule(scheduleObject BaseScheduleObject) {
	scheduler.ids <- scheduleObject.GetId()
}

func (scheduler *PlanScheduler) Execute() bool {
	ctx := scheduler.GetContext()

	now := time.Now()
	waitDuration := time.Minute

	// 检查堆中对象
	if scheduleObject := scheduler.scheduleObjectHeap.Top(); scheduleObject != nil {
		scheduler.Info("TopHeap",
			zap.Time("PlannedScheduleTime", scheduleObject.GetPlannedScheduleTime()),
		)
		if scheduleObject.GetPlannedScheduleTime().Before(now) {
			scheduler.scheduleObjectHeap.Dequeue()
			// 调度核心处理逻辑
			err := scheduler.handlePlanScheduleObjectById(ctx, scheduleObject.GetId())
			if err != nil {
				scheduler.Warn("HandlePlanScheduleObjectByIdFailed",
					zap.Error(err),
				)
			}
			return true
		}

		if d := scheduleObject.GetPlannedScheduleTime().Sub(now); d < waitDuration {
			waitDuration = d
		}
	}
	if id := scheduler.pendingId; id != "" {
		scheduler.Info("GetId",
			zap.String("ScheduleObjectId", id),
		)
		// 重置pendingId，防止死循环
		scheduler.pendingId = ""
		// 调度核心处理逻辑
		err := scheduler.handlePlanScheduleObjectById(ctx, id)
		if err != nil {
			scheduler.Warn("HandlePlanScheduleObjectByIdFailed",
				zap.Error(err),
			)
		}
		return true
	}

	select {
	case id := <-scheduler.ids:
		scheduler.pendingId = id
	case <-time.After(waitDuration):
	case <-ctx.Done():
	}
	return true
}

func (scheduler *PlanScheduler) PreExecute() bool {
	scheduler.Info("PlanSchedulerReady")
	return true
}

func (scheduler *PlanScheduler) CleanUp() {
}

// 对调度对象进行处理
func (scheduler *PlanScheduler) handlePlanScheduleObjectById(ctx context.Context, id string) error {
	// 查库获取未处理的plan类型的调度对象
	scheduleObject, err := scheduler.manager.FindPlanScheduleObjectById(ctx, id)
	if err != nil {
		scheduler.Warn("FindPlanScheduleObjectByIdFailed",
			zap.Error(err),
		)
		return err
	}
	nowTime := time.Now().Local()

	if scheduleObject == nil {
		return nil
	}

	// 调度时间为空就丢弃
	if scheduleObject.GetPlannedScheduleTime().IsZero() {
		return nil
	}

	// 判断是否能立即执行
	if scheduleObject.GetPlannedScheduleTime().Before(nowTime) {
		err = scheduler.worker.Work(scheduleObject)
		if err != nil {
			scheduler.Warn("PlanSchedulerWorkFailed",
				zap.Error(err),
			)
			return err
		}
		// 成功通知后就更新通知状态
		err = scheduler.manager.UpdatePlanScheduleObjectStatusById(ctx, scheduleObject.GetId(), true)
		if err != nil {
			scheduler.Warn("UpdateHandledStatusInPlanByIdFailed",
				zap.Error(err),
			)
		}
		return err
	}

	// 判断计划通知时间是否不大于当前+addHeapDuration，来决定是否入堆
	if scheduleObject.GetPlannedScheduleTime().Before(nowTime.Add(scheduler.addHeapDuration)) {
		scheduler.scheduleObjectHeap.Push(scheduleObject)
	}
	return nil
}
