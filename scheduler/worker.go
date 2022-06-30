package scheduler

import (
	"go.uber.org/zap"
	"schedule-test/lib/log"
)

type Worker struct {
	*zap.Logger
}

func NewWorker() *Worker {

	return &Worker{
		Logger: log.Logger.Get().Named("Worker"),
	}
}

func (worker *Worker) Work(scheduleObject BaseScheduleObject) error {
	err := scheduleObject.Do(nil)
	if err != nil {
		worker.Warn("WorkFailed",
			zap.Error(err),
		)
		return err
	}
	return nil
}
