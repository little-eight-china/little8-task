package test

import (
	"schedule-test/scheduler"
	"testing"
	"time"
)

type ScheduleTest struct {
	scheduler *scheduler.Scheduler

	worker *SchedulerObjectTestWorker
}

func NewScheduleTest(object *SchedulerObjectTest) (*ScheduleTest, error) {

	service := &SchedulerObjectTestService{}
	service.SetObject(object)

	scheduleWorker := NewWorker(object, service)

	schedulerWorker := scheduler.NewWorker()

	cronScheduler := scheduler.NewCronScheduler(schedulerWorker, scheduleWorker)
	periodScheduler := scheduler.NewPeriodScheduler(schedulerWorker, scheduleWorker)
	planScheduler, err := scheduler.NewPlanScheduler(0, 0, schedulerWorker, scheduleWorker)
	if err != nil {
		return nil, err
	}

	newScheduler := scheduler.NewScheduler(scheduleWorker.FindScheduleObjectById).
		WithCronSchedule(cronScheduler).
		WithPeriodSchedule(periodScheduler).
		WithPlanSchedule(planScheduler)

	return &ScheduleTest{
		scheduler: newScheduler,
		worker:    scheduleWorker,
	}, nil
}

func (schedule *ScheduleTest) Start() error {
	if err := schedule.scheduler.Start(); err != nil {
		return err
	}
	return nil
}

func (schedule *ScheduleTest) Stop() error {
	if err := schedule.scheduler.Stop(); err != nil {
		return err
	}

	return nil
}

// 测试从 plan-> period -> cron -> plan -> cron -> 删除
func Test(t *testing.T) {
	object := &SchedulerObjectTest{}
	object.Id = "id"
	object.Name = "测试"
	object.ScheduleMode = scheduler.ScheduleModePlan
	object.PlannedScheduleTime = time.Now()

	scheduleTest, _ := NewScheduleTest(object)

	go func() {
		if err := scheduleTest.Start(); err != nil {
			t.Error(err)
			return
		}

		for {
			time.Sleep(time.Second)
		}
	}()

	// 测试plan
	scheduleTest.scheduler.Schedule(object.Id, nil)

	time.Sleep(time.Second * 2)

	// 删除plan，修改成period
	object1 := *object
	object1.ScheduleMode = scheduler.ScheduleModePeriod
	object1.SchedulePeriod = time.Second
	scheduleTest.worker.SetObject(&object1)
	scheduleTest.scheduler.Schedule(object1.Id, nil)

	time.Sleep(time.Second * 2)

	// 删除period，修改成cron
	object2 := object1
	object2.ScheduleMode = scheduler.ScheduleModeCron
	object2.CronSpec = "* * * * * *"
	scheduleTest.worker.SetObject(&object2)
	scheduleTest.scheduler.Schedule(object2.Id, nil)

	time.Sleep(time.Second * 2)

	// 删除cron，修改成plan
	object3 := object2
	object3.ScheduleMode = scheduler.ScheduleModePlan
	scheduleTest.worker.SetObject(&object3)
	scheduleTest.scheduler.Schedule(object3.Id, nil)

	time.Sleep(time.Second * 5)

	// 删除plan，修改成cron
	object4 := object3
	object4.ScheduleMode = scheduler.ScheduleModeCron
	scheduleTest.worker.SetObject(&object4)
	scheduleTest.scheduler.Schedule(object4.Id, nil)

	time.Sleep(time.Second * 5)

	object5 := object4
	object5.Deleted = true
	scheduleTest.worker.SetObject(&object5)
	scheduleTest.scheduler.Schedule(object5.Id, nil)

	time.Sleep(time.Second * 5)
}
