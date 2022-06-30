package test

import (
	"testing"
	"time"

	"schedule-test/task"
)

func TestPlan(t *testing.T) {
	runner, _ := NewPlanTaskTestRunner()
	runner.Start()
	for {
		time.Sleep(time.Second)
	}
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

func TestPlanFactory(t *testing.T) {
	factory := task.NewRunnerFactory().RegisterPlanAdapter()
	runner, _ := factory.GetRunner(NewPlanTaskTest())
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
