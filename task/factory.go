package task

type RunnerFactory struct {
	adapters Adapters
}

func NewRunnerFactory() *RunnerFactory {
	return &RunnerFactory{}
}

func (factory *RunnerFactory) GetRunner(task ExtendedTask) (Runner, error) {
	adapter := factory.getAdapter(task.GetMode())
	return adapter.CreateRunner(task)
}

func (factory *RunnerFactory) getAdapter(mode string) Adapter {
	for _, adapter := range factory.adapters {
		if adapter.Supports(mode) {
			return adapter
		}
	}
	return nil
}

func (factory *RunnerFactory) RegisterCronAdapter() *RunnerFactory {
	factory.adapters = append(factory.adapters, NewCronAdapter())
	return factory
}

func (factory *RunnerFactory) RegisterPeriodAdapter() *RunnerFactory {
	factory.adapters = append(factory.adapters, NewPeriodAdapter())
	return factory
}

func (factory *RunnerFactory) RegisterAllAdapter() *RunnerFactory {
	return factory.RegisterCronAdapter().RegisterPeriodAdapter()
}

type Adapter interface {
	Supports(string) bool
	CreateRunner(ExtendedTask) (Runner, error)
}

type Adapters []Adapter

type CronAdapter struct {
}

func NewCronAdapter() *CronAdapter {
	return &CronAdapter{}
}

func (adapter *CronAdapter) Supports(mode string) bool {
	return mode == ModeCron
}

func (adapter *CronAdapter) CreateRunner(task ExtendedTask) (Runner, error) {
	return NewCronRunner(task.(CronExtendedTask))
}

type PeriodAdapter struct {
}

func NewPeriodAdapter() *PeriodAdapter {
	return &PeriodAdapter{}
}

func (adapter *PeriodAdapter) Supports(mode string) bool {
	return mode == ModePeriod
}

func (adapter *PeriodAdapter) CreateRunner(task ExtendedTask) (Runner, error) {
	return NewPeriodRunner(task.(PeriodExtendedTask))
}
