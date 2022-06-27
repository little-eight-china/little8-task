package test

import (
	"fmt"
	"testing"
	"time"

	"schedule-test/task"
)

type PeriodTaskTest struct {
	*task.DefaultPeriodTask
	task.Runner
}

func (task *PeriodTaskTest) PreExecute() bool {
	return true
}

func (task *PeriodTaskTest) Execute() bool {
	fmt.Println("执行啦！！")
	return true
}

func (task *PeriodTaskTest) CleanUp() error {
	return nil
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
	fmt.Println("执行啦！！")
	return true
}

func (task *CronTaskTest) CleanUp() error {
	return nil
}

func TestCron(t *testing.T) {
	runner, _ := NewCronTaskTestRunner()
	runner.Start()
	for {
		time.Sleep(time.Second)
	}
}

func TestPeriod(t *testing.T) {
	runner, err := NewPeriodTaskTestRunner()
	if err != nil {
		return
	}
	runner.Start()
	for {
		time.Sleep(time.Second)
	}
}

func TestCronFactory(t *testing.T) {
	factory := task.NewRunnerFactory().RegisterCronAdapter()
	runner, _ := factory.GetRunner(NewCronTaskTest())
	runner.Start()
	for {
		time.Sleep(time.Second)
	}
}

func TestPeriodFactory(t *testing.T) {
	factory := task.NewRunnerFactory().RegisterPeriodAdapter()
	runner, _ := factory.GetRunner(NewPeriodTaskTest())
	runner.Start()
	for {
		time.Sleep(time.Second)
	}
}
