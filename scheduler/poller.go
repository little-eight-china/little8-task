package scheduler

import (
	"schedule-test/lib/log"
	"time"

	"go.uber.org/zap"
	"schedule-test/task"
)

const (
	DefaultJobDuration = time.Minute * 5
)

type BasePoller struct {
	schedule func(id string, scheduleObject BaseScheduleObject)
}

func NewBasePoller(schedule func(id string, scheduleObject BaseScheduleObject)) *BasePoller {
	return &BasePoller{
		schedule: schedule,
	}
}

type CronPoller struct {
	*BasePoller
	*zap.Logger

	*task.DefaultPeriodTask
	task.Runner

	manager CronScheduleObjectManager
}

func NewCronPoller(jobDuration time.Duration, manager CronScheduleObjectManager, schedule func(id string, scheduleObject BaseScheduleObject)) (*CronPoller, error) {
	poller := &CronPoller{
		Logger:            log.Logger.Get().Named("CronPoller"),
		BasePoller:        NewBasePoller(schedule),
		DefaultPeriodTask: task.NewDefaultPeriodTask("CronPoller", 5*time.Minute),
		manager:           manager,
	}

	return poller, nil

}

func (poller *CronPoller) PreExecute() bool {
	poller.Info("CronPollerReady")
	return true
}

func (poller *CronPoller) Execute() bool {
	// 获取对应cron类型的调度对象
	scheduleObjects, err := poller.manager.FindCronScheduleObjects(poller.GetContext())
	if err != nil {
		poller.Warn("FindInCronPollerFailed",
			zap.Error(err),
		)
		return true
	}

	poller.Info("CronFetch",
		zap.Int("ScheduleObjects", len(scheduleObjects)),
	)

	// 遍历给调度器
	for _, scheduleObject := range scheduleObjects {
		poller.schedule(scheduleObject.GetId(), scheduleObject)
	}
	return true
}

func (poller *CronPoller) CleanUp() error {
	return nil
}

type PeriodPoller struct {
	*BasePoller
	*zap.Logger

	*task.DefaultPeriodTask
	task.Runner

	manager PeriodScheduleObjectManager
}

func NewPeriodPoller(jobDuration time.Duration, manager PeriodScheduleObjectManager,
	schedule func(id string, scheduleObject BaseScheduleObject)) (*PeriodPoller, error) {
	poller := &PeriodPoller{
		Logger:            log.Logger.Get().Named("PeriodPoller"),
		BasePoller:        NewBasePoller(schedule),
		DefaultPeriodTask: task.NewDefaultPeriodTask("PeriodPoller", 5*time.Minute),
		manager:           manager,
	}

	if jobDuration == 0 {
		jobDuration = DefaultJobDuration
	}

	return poller, nil

}

func (poller *PeriodPoller) PreExecute() bool {
	poller.Info("PeriodPollerReady")
	return true
}

func (poller *PeriodPoller) Execute() bool {
	// 获取period的调度对象
	scheduleObjects, err := poller.manager.FindPeriodScheduleObjects(poller.GetContext())
	if err != nil {
		poller.Warn("FindInPeriodPollerFailed",
			zap.Error(err),
		)
		return true
	}

	poller.Info("PeriodFetch",
		zap.Int("ScheduleObjects", len(scheduleObjects)),
	)
	// 遍历给调度器
	for _, scheduleObject := range scheduleObjects {
		poller.schedule(scheduleObject.GetId(), scheduleObject)
	}

	return true
}

func (poller *PeriodPoller) CleanUp() error {
	return nil
}

type PlanPoller struct {
	*BasePoller
	*zap.Logger

	*task.DefaultPeriodTask
	task.Runner

	manager PlanScheduleObjectManager
}

func NewPlanPoller(manager PlanScheduleObjectManager, schedule func(id string, scheduleObject BaseScheduleObject)) (*PlanPoller, error) {
	poller := &PlanPoller{
		Logger:            log.Logger.Get().Named("PlanPoller"),
		BasePoller:        NewBasePoller(schedule),
		DefaultPeriodTask: task.NewDefaultPeriodTask("PlanPoller", 5*time.Second),
		manager:           manager,
	}

	return poller, nil
}

func (poller *PlanPoller) PreExecute() bool {
	poller.Info("PlanPollerReady")
	return true
}

func (poller *PlanPoller) Execute() bool {
	// 获取可执行的plan调度对象
	scheduleObjects, err := poller.manager.FindPlanScheduleObjects(poller.GetContext())
	if err != nil {
		poller.Warn("FindInPlanPollerFailed",
			zap.Error(err),
		)
		return true
	}

	poller.Info("PlanFetch",
		zap.Int("ScheduleObjects", len(scheduleObjects)),
	)
	// 遍历给调度器调配
	for _, scheduleObject := range scheduleObjects {
		poller.schedule(scheduleObject.GetId(), scheduleObject)
	}
	return true
}

func (poller *PlanPoller) CleanUp() error {
	return nil
}
