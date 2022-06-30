package scheduler

import (
	"container/heap"
	"context"
	"time"
)

type BaseScheduleObject interface {
	GetId() string
	GetName() string
	GetScheduleMode() string
	GetVersion() int
	IsDeleted() bool
	Do(context.Context) error

	IsCronMode() (CronScheduleObject, bool)
	IsPeriodMode() (PeriodScheduleObject, bool)
	IsPlanMode() (PlanScheduleObject, bool)
}
type BaseScheduleObjects []BaseScheduleObject

type CronScheduleObject interface {
	BaseScheduleObject
	GetCronSpec() string
}

type CronScheduleObjects []CronScheduleObject

func (objects CronScheduleObjects) Append(others ...CronScheduleObject) CronScheduleObjects {
	return append(objects, others...)
}

type PeriodScheduleObject interface {
	BaseScheduleObject
	GetSchedulePeriod() time.Duration
}

type PeriodScheduleObjects []PeriodScheduleObject

func (objects PeriodScheduleObjects) Append(others ...PeriodScheduleObject) PeriodScheduleObjects {
	return append(objects, others...)
}

type PlanScheduleObject interface {
	BaseScheduleObject
	GetPlannedScheduleTime() time.Time
}

type PlanScheduleObjects []PlanScheduleObject

func (objects PlanScheduleObjects) Append(others ...PlanScheduleObject) PlanScheduleObjects {
	return append(objects, others...)
}

type PlanScheduleObjectHeapByTime PlanScheduleObjects

func (h PlanScheduleObjectHeapByTime) Native() PlanScheduleObjects {
	return PlanScheduleObjects(h)
}

func (h PlanScheduleObjectHeapByTime) Len() int {
	return len(h)
}

func (h PlanScheduleObjectHeapByTime) Less(i, j int) bool {
	return h[i].GetPlannedScheduleTime().Before(h[j].GetPlannedScheduleTime())
}

func (h PlanScheduleObjectHeapByTime) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *PlanScheduleObjectHeapByTime) Push(x interface{}) {
	*h = append(*h, x.(PlanScheduleObject))
}

func (h *PlanScheduleObjectHeapByTime) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h PlanScheduleObjectHeapByTime) Empty() bool {
	return h.Len() == 0
}

func (h PlanScheduleObjectHeapByTime) Top() PlanScheduleObject {
	if h.Empty() {
		return nil
	}

	return h[0]
}

func (h *PlanScheduleObjectHeapByTime) Enqueue(s PlanScheduleObject) {
	heap.Push(h, s)
}

func (h *PlanScheduleObjectHeapByTime) Dequeue() PlanScheduleObject {
	return heap.Pop(h).(PlanScheduleObject)
}

type BaseScheduleObjectManager interface {
	FindScheduleObjectById(ctx context.Context, id string) (BaseScheduleObject, error)
}

type CronScheduleObjectManager interface {
	BaseScheduleObjectManager

	FindCronScheduleObjects(ctx context.Context) (CronScheduleObjects, error)
	FindCronScheduleObjectById(ctx context.Context, id string) (CronScheduleObject, error)
}

type PeriodScheduleObjectManager interface {
	BaseScheduleObjectManager

	FindPeriodScheduleObjects(ctx context.Context) (PeriodScheduleObjects, error)
	FindPeriodScheduleObjectById(ctx context.Context, id string) (PeriodScheduleObject, error)
}

type PlanScheduleObjectManager interface {
	BaseScheduleObjectManager

	FindPlanScheduleObjects(ctx context.Context) (PlanScheduleObjects, error)
	FindPlanScheduleObjectById(ctx context.Context, id string) (PlanScheduleObject, error)
	// UpdatePlanScheduleObjectStatusById 根据id更新已处理的plan类型的调度对象的状态, status为true则说明处理成功，反之处理失败
	UpdatePlanScheduleObjectStatusById(ctx context.Context, id string, status bool) error
}
