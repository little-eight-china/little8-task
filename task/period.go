package task

import (
	"time"
)

type PeriodRunner struct {
	PeriodExtendedTask
	Runner
}

func NewPeriodRunner(task PeriodExtendedTask) (*PeriodRunner, error) {

	periodRunner := PeriodRunner{
		PeriodExtendedTask: task,
	}

	runner := NewDefaultRunner(&periodRunner)

	periodRunner.Runner = runner

	return &periodRunner, nil
}

func (runner *PeriodRunner) Run() {
	if runner.IsCanceled() {
		return
	}
	if !runner.PreExecute() {
		return
	}

StartScheduling:

	currentTime := time.Now()

	if runner.GetAlign() && runner.GetStrictAlign() {
		runner.SetNextExecuteTime(alignNextTime(currentTime, runner.GetInterval(), runner.GetOffset()))
	} else {
		runner.SetNextExecuteTime(currentTime)
	}

PeriodRunnerLoop:
	for {
		currentTime = time.Now()

		if currentTime.Before(runner.GetNextExecuteTime()) {

			waitDuration := runner.GetNextExecuteTime().Sub(currentTime)
			if runner.GetAlign() && waitDuration > runner.GetInterval() {
				runner.SetNextExecuteTime(alignNextTime(currentTime, runner.GetInterval(), runner.GetOffset()))
				continue
			}

			select {
			case <-runner.GetRescheduleSignal():
				goto StartScheduling
			case <-time.After(waitDuration):
			case <-runner.GetContext().Done():
				break PeriodRunnerLoop
			}
		} else {
			if runner.IsCanceled() {
				break
			}
			if !runner.Execute() {
				break
			}

			if runner.GetAlign() {
				runner.SetNextExecuteTime(alignNextTime(currentTime, runner.GetInterval(), runner.GetOffset()))
			} else {
				runner.SetNextExecuteTime(runner.GetNextExecuteTime().Add(runner.GetInterval()))
			}
		}
	}
	runner.CleanUp()
	return
}

func alignNextTime(base time.Time, interval time.Duration, offset time.Duration) time.Time {
	result := base.Truncate(interval).Add(offset)
	for result.Before(base) {
		result = result.Add(interval)
	}
	return result
}
