package resource

import (
	"context"
	"github.com/concourse/concourse/atc/runtime"

	"github.com/concourse/concourse/atc"
)

//type putRequest struct {
//	Source atc.Source `json:"source"`
//	Params atc.Params `json:"params,omitempty"`
//}

func (resource *resource) Put(
	ctx context.Context,
	ioConfig runtime.IOConfig,
	source atc.Source,
	params atc.Params,
) (runtime.VersionResult, error) {
	resourceDir := ResourcesDir("put")

	vr := &runtime.VersionResult{}

	err := resource.runScript(
		ctx,
		"/opt/resource/out",
		[]string{resourceDir},
		runtime.PutRequest{
			Params: params,
			Source: source,
		},
		&vr,
		ioConfig.Stderr,
		true,
	)
	if err != nil {
		return runtime.VersionResult{}, err
	}

	return *vr, nil
}
