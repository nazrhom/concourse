package exec

import (
	"io"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/db"
)

//go:generate counterfeiter . BuildStepDelegate

type BuildStepDelegate interface {
	ImageVersionDetermined(db.UsedResourceCache) error

	Stdout() io.Writer
	Stderr() io.Writer

	Initializing(lager.Logger)
	Starting(lager.Logger)
	Finished(lager.Logger, bool)
	Errored(lager.Logger, string)
}
