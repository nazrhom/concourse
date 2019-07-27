package exec

import (
	"context"
	"fmt"
	"io/ioutil"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagerctx"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/db"
	"github.com/ghodss/yaml"
)

type SetPipelineStep struct {
	plan        atc.Plan
	teamFactory db.TeamFactory
	build       db.Build
	delegate    BuildStepDelegate
	succeeded   bool
}

func NewSetPipelineStep(plan atc.Plan, build db.Build, teamFactory db.TeamFactory, delegate BuildStepDelegate) Step {
	return &SetPipelineStep{
		plan:        plan,
		teamFactory: teamFactory,
		build:       build,
		delegate:    delegate,
	}
}

func (step *SetPipelineStep) Run(ctx context.Context, state RunState) error {
	logger := lagerctx.FromContext(ctx).WithData(lager.Data{
		"plan-id": step.plan.ID,
	})

	step.delegate.Initializing(logger)

	name := step.plan.SetPipeline.Name
	file := step.plan.SetPipeline.File

	team := step.teamFactory.GetByID(step.build.TeamID())
	artifacts := state.Artifacts()

	rc, err := artifacts.StreamFile(logger, file)
	if err != nil {
		return err
	}

	defer rc.Close()

	configBytes, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	step.delegate.Starting(logger)

	// TODO: mapstructure decode hooks and all that messy stuff
	var config atc.Config
	err = yaml.Unmarshal(configBytes, &config)
	if err != nil {
		return err
	}

	warnings, errors := config.Validate()

	for _, warning := range warnings {
		fmt.Fprintf(step.delegate.Stderr(), "WARNING: %s\n", warning.Message)
	}

	if len(errors) > 0 {
		fmt.Fprintln(step.delegate.Stderr(), "invalid pipeline:")

		for _, e := range errors {
			fmt.Fprintf(step.delegate.Stderr(), "- %s", e)
		}

		step.succeeded = false
		step.delegate.Finished(logger, false)
		return nil
	}

	pipeline, found, err := team.Pipeline(name)
	if err != nil {
		return err
	}

	var fromVersion db.ConfigVersion
	if found {
		fromVersion = pipeline.ConfigVersion()

		// TODO: diff?
	}

	pipeline, created, err := team.SavePipeline(name, config, fromVersion, false)
	if err != nil {
		// TODO: handle 'from' version race?
		step.delegate.Finished(logger, true)

		return err
	}

	if created {
		fmt.Fprintln(step.delegate.Stdout(), "pipeline created")
	} else {
		fmt.Fprintln(step.delegate.Stdout(), "pipeline updated")
	}

	step.delegate.Finished(logger, true)

	step.succeeded = true

	return nil
}

func (step *SetPipelineStep) Succeeded() bool {
	return step.succeeded
}
