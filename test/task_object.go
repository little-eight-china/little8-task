package test

import (
	"fmt"
	"time"

	"schedule-test/task"
)

type PlanTaskTest struct {
	*task.DefaultTask
	task.Runner
}

func (task *PlanTaskTest) PreExecute() bool {
	return true
}

func (task *PlanTaskTest) Execute() bool {
	fmt.Println("plan执行啦！！")
	return true
}

func (task *PlanTaskTest) CleanUp() {
}

func NewPlanTaskTest() *PlanTaskTest {
	planTask := &PlanTaskTest{
		DefaultTask: task.NewDefaultTask("NewPlanTaskTest"),
	}
	return planTask
}

func NewPlanTaskTestRunner() (task.Runner, error) {
	planRunner := &PlanTaskTest{
		DefaultTask: task.NewDefaultTask("NewPlanTaskTest"),
	}

	runner, _ := task.NewPlanRunner(planRunner)

	planRunner.Runner = runner

	return planRunner, nil
}

type PeriodTaskTest struct {
	*task.DefaultPeriodTask
	task.Runner
}

func (task *PeriodTaskTest) PreExecute() bool {
	return true
}

func (task *PeriodTaskTest) Execute() bool {
	fmt.Println("period执行啦！！")
	return true
}

func (task *PeriodTaskTest) CleanUp() {
}

func NewPeriodTaskTest() *PeriodTaskTest {
	periodTask := &PeriodTaskTest{
		DefaultPeriodTask: task.NewDefaultPeriodTask("NewPeriodTaskTest", time.Second),
	}

	return periodTask
}

func NewPeriodTaskTestRunner() (task.Runner, error) {
	periodTask := &PeriodTaskTest{
		DefaultPeriodTask: task.NewDefaultPeriodTask("NewPeriodTaskTest", time.Second),
	}

	runner, err := task.NewPeriodRunner(periodTask)
	if err != nil {
		return nil, err
	}

	periodTask.Runner = runner
	return periodTask, nil
}

type CronTaskTest struct {
	*task.DefaultCronTask
	task.Runner
}

func NewCronTaskTest() *CronTaskTest {
	cronTask := &CronTaskTest{
		DefaultCronTask: task.NewDefaultCronTask("NewConTask", "* * * * * *"),
	}

	return cronTask
}

func NewCronTaskTestRunner() (task.Runner, error) {
	cronTask := &CronTaskTest{
		DefaultCronTask: task.NewDefaultCronTask("NewConTask", "* * * * * *"),
	}

	runner, err := task.NewCronRunner(cronTask)
	if err != nil {
		return nil, err
	}

	cronTask.Runner = runner
	return cronTask, nil
}

func (task *CronTaskTest) PreExecute() bool {
	return true
}

func (task *CronTaskTest) Execute() bool {
	fmt.Println("cron执行啦！！")
	return true
}

func (task *CronTaskTest) CleanUp() {
}
