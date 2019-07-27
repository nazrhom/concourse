package atc

type JobConfig struct {
	Name    string `json:"name"`
	OldName string `json:"old_name,omitempty"`
	Public  bool   `json:"public,omitempty"`

	DisableManualTrigger bool     `json:"disable_manual_trigger,omitempty"`
	Serial               bool     `json:"serial,omitempty"`
	Interruptible        bool     `json:"interruptible,omitempty"`
	SerialGroups         []string `json:"serial_groups,omitempty"`
	RawMaxInFlight       int      `json:"max_in_flight,omitempty"`
	BuildLogsToRetain    int      `json:"build_logs_to_retain,omitempty"`

	BuildLogRetention *BuildLogRetention `json:"build_log_retention,omitempty"`

	OnAbort   *PlanConfig `json:"on_abort,omitempty"`
	OnError   *PlanConfig `json:"on_error,omitempty"`
	OnFailure *PlanConfig `json:"on_failure,omitempty"`
	OnSuccess *PlanConfig `json:"on_success,omitempty"`
	Ensure    *PlanConfig `json:"ensure,omitempty"`

	Plan PlanSequence `json:"plan"`
}

type BuildLogRetention struct {
	Builds int `json:"builds,omitempty"`
	Days   int `json:"days,omitempty"`
}

func (config JobConfig) Hooks() Hooks {
	return Hooks{
		Abort:   config.OnAbort,
		Error:   config.OnError,
		Failure: config.OnFailure,
		Success: config.OnSuccess,
		Ensure:  config.Ensure,
	}
}

func (config JobConfig) MaxInFlight() int {
	if config.Serial || len(config.SerialGroups) > 0 {
		return 1
	}

	if config.RawMaxInFlight != 0 {
		return config.RawMaxInFlight
	}

	return 0
}

func (config JobConfig) GetSerialGroups() []string {
	if len(config.SerialGroups) > 0 {
		return config.SerialGroups
	}

	if config.Serial || config.RawMaxInFlight > 0 {
		return []string{config.Name}
	}

	return []string{}
}

func (config JobConfig) Plans() []PlanConfig {
	plan := collectPlans(PlanConfig{
		Do:        &config.Plan,
		OnAbort:   config.OnAbort,
		OnError:   config.OnError,
		OnFailure: config.OnFailure,
		OnSuccess: config.OnSuccess,
		Ensure:    config.Ensure,
	})

	return plan
}

func collectPlans(plan PlanConfig) []PlanConfig {
	var plans []PlanConfig

	if plan.OnAbort != nil {
		plans = append(plans, collectPlans(*plan.OnAbort)...)
	}

	if plan.OnError != nil {
		plans = append(plans, collectPlans(*plan.OnError)...)
	}

	if plan.OnSuccess != nil {
		plans = append(plans, collectPlans(*plan.OnSuccess)...)
	}

	if plan.OnFailure != nil {
		plans = append(plans, collectPlans(*plan.OnFailure)...)
	}

	if plan.Ensure != nil {
		plans = append(plans, collectPlans(*plan.Ensure)...)
	}

	if plan.Try != nil {
		plans = append(plans, collectPlans(*plan.Try)...)
	}

	if plan.Do != nil {
		for _, p := range *plan.Do {
			plans = append(plans, collectPlans(p)...)
		}
	}

	if plan.Aggregate != nil {
		for _, p := range *plan.Aggregate {
			plans = append(plans, collectPlans(p)...)
		}
	}

	if plan.InParallel != nil {
		for _, p := range plan.InParallel.Steps {
			plans = append(plans, collectPlans(p)...)
		}
	}

	return append(plans, plan)
}

func (config JobConfig) InputPlans() []PlanConfig {
	var inputs []PlanConfig

	for _, plan := range config.Plans() {
		if plan.Get != "" {
			inputs = append(inputs, plan)
		}
	}

	return inputs
}

func (config JobConfig) OutputPlans() []PlanConfig {
	var outputs []PlanConfig

	for _, plan := range config.Plans() {
		if plan.Put != "" {
			outputs = append(outputs, plan)
		}
	}

	return outputs
}

func (config JobConfig) Inputs() []JobInput {
	var inputs []JobInput

	for _, plan := range config.Plans() {
		if plan.Get != "" {
			get := plan.Get

			resource := get
			if plan.Resource != "" {
				resource = plan.Resource
			}

			inputs = append(inputs, JobInput{
				Name:     get,
				Resource: resource,
				Passed:   plan.Passed,
				Version:  plan.Version,
				Trigger:  plan.Trigger,
				Params:   plan.Params,
				Tags:     plan.Tags,
			})
		}
	}

	return inputs
}

func (config JobConfig) Outputs() []JobOutput {
	var outputs []JobOutput

	for _, plan := range config.Plans() {
		if plan.Put != "" {
			put := plan.Put

			resource := put
			if plan.Resource != "" {
				resource = plan.Resource
			}

			outputs = append(outputs, JobOutput{
				Name:     put,
				Resource: resource,
			})
		}
	}

	return outputs
}
