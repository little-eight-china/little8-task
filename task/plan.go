package task

type PlanRunner struct {
	ExtendedTask
	Runner
}

func NewPlanRunner(task ExtendedTask) (*PlanRunner, error) {

	planRunner := PlanRunner{
		ExtendedTask: task,
	}

	runner := NewDefaultRunner(&planRunner)

	planRunner.Runner = runner

	return &planRunner, nil
}

func (runner *PlanRunner) Run() {
	if runner.IsCanceled() {
		return
	}
	if !runner.PreExecute() {
		return
	}

	if !runner.Execute() {
		return
	}

	runner.CleanUp()
	return
}
