package task

import (
	"errors"
)

type Runner interface {
	Start() error
	Shutdown() error
	Join() error
	Stop() error
}

type DefaultRunner struct {
	AbstractTask

	mutexChan chan struct{}
	enterChan chan struct{}
	exitChan  chan struct{}
}

func NewDefaultRunner(task AbstractTask) *DefaultRunner {
	return &DefaultRunner{
		mutexChan: make(chan struct{}, 1),
		enterChan: make(chan struct{}),
		exitChan:  make(chan struct{}),

		AbstractTask: task,
	}
}

func (runner *DefaultRunner) Start() error {
	runner.mutexChan <- struct{}{}

	select {
	case <-runner.enterChan:
		<-runner.mutexChan
		return errors.New("task reentry")
	default:
	}

	go func() {
		close(runner.enterChan)
		<-runner.mutexChan

		defer close(runner.exitChan)
	}()

	return runner.Run()
}

func (runner *DefaultRunner) Shutdown() error {
	runner.Cancel()
	return nil
}

func (runner *DefaultRunner) Join() error {
	select {
	case <-runner.exitChan:
	}

	return nil
}

func (runner *DefaultRunner) Stop() error {
	if err := runner.Shutdown(); err != nil {
		return err
	}

	if err := runner.Join(); err != nil {
		return err
	}

	return nil
}
