package task

import (
	"time"

	"github.com/robfig/cron/v3"
)

var DefaultParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type CronRunner struct {
	CronExtendedTask
	Runner

	schedule cron.Schedule

	lastExecuteTime time.Time
	nextExecuteTime time.Time
}

func NewCronRunner(task CronExtendedTask) (*CronRunner, error) {
	schedule, err := DefaultParser.Parse(task.GetCronSpec())
	if err != nil {
		return nil, err
	}

	cronRunner := CronRunner{
		CronExtendedTask: task,
		schedule:         schedule,
	}

	runner := NewDefaultRunner(&cronRunner)

	cronRunner.Runner = runner

	return &cronRunner, nil
}

func (runner *CronRunner) Run() error {
	if runner.IsCanceled() {
		return nil
	}
	if !runner.PreExecute() {
		return nil
	}

	runner.lastExecuteTime = time.Now()

CronRunnerLoop:
	for {
		currentTime := time.Now()
		runner.nextExecuteTime = runner.schedule.Next(runner.lastExecuteTime)

		if currentTime.Before(runner.nextExecuteTime) {
			waitDuration := runner.nextExecuteTime.Sub(currentTime)
			select {
			case <-time.After(waitDuration):
			case <-runner.GetContext().Done():
				break CronRunnerLoop
			}

		} else {
			if runner.IsCanceled() {
				break
			}
			if !runner.Execute() {
				break
			}

			runner.lastExecuteTime = time.Now()
		}
	}

	return runner.CleanUp()
}
