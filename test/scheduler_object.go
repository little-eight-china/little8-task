package test

import (
	"context"
	"fmt"
	"schedule-test/scheduler"
	"time"
)

type SchedulerObjectTest struct {
	Id                  string
	Name                string
	ScheduleMode        string
	Version             int
	Deleted             bool
	CronSpec            string
	SchedulePeriod      time.Duration
	PlannedScheduleTime time.Time

	Status bool
}

type SchedulerObjectTests []*SchedulerObjectTest

func (object *SchedulerObjectTest) GetId() string {
	return object.Id
}

func (object *SchedulerObjectTest) GetName() string {
	return object.Name
}

func (object *SchedulerObjectTest) GetScheduleMode() string {
	return object.ScheduleMode
}

func (object *SchedulerObjectTest) GetVersion() int {
	return object.Version
}

func (object *SchedulerObjectTest) IsDeleted() bool {
	return object.Deleted
}

func (object *SchedulerObjectTest) GetCronSpec() string {
	return object.CronSpec
}

func (object *SchedulerObjectTest) GetSchedulePeriod() time.Duration {
	return object.SchedulePeriod
}

func (object *SchedulerObjectTest) GetPlannedScheduleTime() time.Time {
	return object.PlannedScheduleTime
}

type SchedulerObjectTestService struct {
	objects SchedulerObjectTests
}

func (service *SchedulerObjectTestService) GetById(id string) (*SchedulerObjectTest, error) {
	return service.objects[0], nil
}

func (service *SchedulerObjectTestService) SetObject(object *SchedulerObjectTest) {
	object.Status = false
	object.Version++
	if service.objects == nil {
		service.objects = make(SchedulerObjectTests, 1)
	}
	service.objects[0] = object
}

func (service *SchedulerObjectTestService) Handle() error {
	fmt.Println(fmt.Sprintf("[%s]SchedulerObjectTestService执行实际逻辑", service.objects[0].ScheduleMode))
	return nil
}

func (service *SchedulerObjectTestService) FindScheduleObjectById(ctx context.Context, id string) (scheduler.BaseScheduleObject, error) {
	object, err := service.GetById(id)
	if err != nil {
		return nil, err
	}
	return NewWorker(object, service), nil
}

func (service *SchedulerObjectTestService) FindCronScheduleObjectById(ctx context.Context, id string) (scheduler.CronScheduleObject, error) {
	object, err := service.GetById(id)
	if err != nil {
		return nil, err
	}
	return NewWorker(object, service), nil
}

func (service *SchedulerObjectTestService) FindPeriodScheduleObjectById(ctx context.Context, id string) (scheduler.PeriodScheduleObject, error) {
	object, err := service.GetById(id)
	if err != nil {
		return nil, err
	}
	return NewWorker(object, service), nil
}

func (service *SchedulerObjectTestService) FindPlanScheduleObjectById(ctx context.Context, id string) (scheduler.PlanScheduleObject, error) {
	object, err := service.GetById(id)
	if err != nil {
		return nil, err
	}
	if object.Status {
		return nil, nil
	}
	return NewWorker(object, service), nil
}

func (service *SchedulerObjectTestService) UpdatePlanScheduleObjectStatusById(ctx context.Context, id string, status bool) error {
	service.objects[0].Status = status
	return nil
}

func (service *SchedulerObjectTestService) FindPlanScheduleObjects(ctx context.Context) (scheduler.PlanScheduleObjects, error) {
	return nil, nil
}

func (service *SchedulerObjectTestService) FindCronScheduleObjects(ctx context.Context) (scheduler.CronScheduleObjects, error) {
	return nil, nil
}

func (service *SchedulerObjectTestService) FindPeriodScheduleObjects(ctx context.Context) (scheduler.PeriodScheduleObjects, error) {
	return nil, nil
}

type SchedulerObjectTestWorker struct {
	*SchedulerObjectTest
	*SchedulerObjectTestService
}

func NewWorker(object *SchedulerObjectTest, service *SchedulerObjectTestService) *SchedulerObjectTestWorker {
	return &SchedulerObjectTestWorker{
		SchedulerObjectTest:        object,
		SchedulerObjectTestService: service,
	}
}

func (worker *SchedulerObjectTestWorker) Do(ctx context.Context) error {
	return worker.Handle()
}

func (worker *SchedulerObjectTestWorker) IsCronMode() (scheduler.CronScheduleObject, bool) {
	if worker.ScheduleMode == scheduler.ScheduleModeCron {
		return worker, true
	}
	return nil, false
}

func (worker *SchedulerObjectTestWorker) IsPeriodMode() (scheduler.PeriodScheduleObject, bool) {
	if worker.ScheduleMode == scheduler.ScheduleModePeriod {
		return worker, true
	}
	return nil, false
}

func (worker *SchedulerObjectTestWorker) IsPlanMode() (scheduler.PlanScheduleObject, bool) {
	if worker.ScheduleMode == scheduler.ScheduleModePlan {
		return worker, true
	}
	return nil, false
}
