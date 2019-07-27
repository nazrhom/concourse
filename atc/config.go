package atc

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/ssh"
)

const ConfigVersionHeader = "X-Concourse-Config-Version"
const DefaultPipelineName = "main"
const DefaultTeamName = "main"

type Tags []string

type Config struct {
	Groups        GroupConfigs    `json:"groups,omitempty"`
	Resources     ResourceConfigs `json:"resources,omitempty"`
	ResourceTypes ResourceTypes   `json:"resource_types,omitempty"`
	Jobs          JobConfigs      `json:"jobs,omitempty"`
}

type GroupConfig struct {
	Name      string   `json:"name"`
	Jobs      []string `json:"jobs,omitempty"`
	Resources []string `json:"resources,omitempty"`
}

type GroupConfigs []GroupConfig

func (groups GroupConfigs) Lookup(name string) (GroupConfig, int, bool) {
	for index, group := range groups {
		if group.Name == name {
			return group, index, true
		}
	}

	return GroupConfig{}, -1, false
}

type ResourceConfig struct {
	Name         string  `json:"name"`
	Public       bool    `json:"public,omitempty"`
	WebhookToken string  `json:"webhook_token,omitempty"`
	Type         string  `json:"type"`
	Source       Source  `json:"source"`
	CheckEvery   string  `json:"check_every,omitempty"`
	CheckTimeout string  `json:"check_timeout,omitempty"`
	Tags         Tags    `json:"tags,omitempty"`
	Version      Version `json:"version,omitempty"`
	Icon         string  `json:"icon,omitempty"`
}

type ResourceType struct {
	Name                 string `json:"name"`
	Type                 string `json:"type"`
	Source               Source `json:"source"`
	Privileged           bool   `json:"privileged,omitempty"`
	CheckEvery           string `json:"check_every,omitempty"`
	Tags                 Tags   `json:"tags,omitempty"`
	Params               Params `json:"params,omitempty"`
	CheckSetupError      string `json:"check_setup_error,omitempty"`
	CheckError           string `json:"check_error,omitempty"`
	UniqueVersionHistory bool   `json:"unique_version_history,omitempty"`
}

type ResourceTypes []ResourceType

func (types ResourceTypes) Lookup(name string) (ResourceType, bool) {
	for _, t := range types {
		if t.Name == name {
			return t, true
		}
	}

	return ResourceType{}, false
}

func (types ResourceTypes) Without(name string) ResourceTypes {
	newTypes := ResourceTypes{}
	for _, t := range types {
		if t.Name != name {
			newTypes = append(newTypes, t)
		}
	}

	return newTypes
}

type Hooks struct {
	Abort   *PlanConfig
	Error   *PlanConfig
	Failure *PlanConfig
	Ensure  *PlanConfig
	Success *PlanConfig
}

// A PlanSequence corresponds to a chain of Compose plan, with an implicit
// `on: [success]` after every Task plan.
type PlanSequence []PlanConfig

// A VersionConfig represents the choice to include every version of a
// resource, the latest version of a resource, or a pinned (specific) one.
type VersionConfig struct {
	Every  bool
	Latest bool
	Pinned Version
}

func (c *VersionConfig) UnmarshalJSON(version []byte) error {
	var data interface{}

	err := json.Unmarshal(version, &data)
	if err != nil {
		return err
	}

	switch actual := data.(type) {
	case string:
		c.Every = actual == "every"
		c.Latest = actual == "latest"
	case map[string]interface{}:
		version := Version{}

		for k, v := range actual {
			if s, ok := v.(string); ok {
				version[k] = strings.TrimSpace(s)
			}
		}

		c.Pinned = version
	default:
		return errors.New("unknown type for version")
	}

	return nil
}

const VersionLatest = "latest"
const VersionEvery = "every"

func (c *VersionConfig) MarshalJSON() ([]byte, error) {
	if c.Latest {
		return json.Marshal(VersionLatest)
	}

	if c.Every {
		return json.Marshal(VersionEvery)
	}

	if c.Pinned != nil {
		return json.Marshal(c.Pinned)
	}

	return json.Marshal("")
}

// A InputsConfig represents the choice to include every artifact within the
// job as an input to the put step or specific ones.
type InputsConfig struct {
	All       bool
	Specified []string
}

func (c *InputsConfig) UnmarshalJSON(inputs []byte) error {
	var data interface{}

	err := json.Unmarshal(inputs, &data)
	if err != nil {
		return err
	}

	switch actual := data.(type) {
	case string:
		c.All = actual == "all"
	case []interface{}:
		inputs := []string{}

		for _, v := range actual {
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("non-string put input: %v", v)
			}

			inputs = append(inputs, strings.TrimSpace(str))
		}

		c.Specified = inputs
	default:
		return errors.New("unknown type for put inputs")
	}

	return nil
}

const InputsAll = "all"

func (c InputsConfig) MarshalJSON() ([]byte, error) {
	if c.All {
		return json.Marshal(InputsAll)
	}

	if c.Specified != nil {
		return json.Marshal(c.Specified)
	}

	return json.Marshal("")
}

type InParallelConfig struct {
	Steps    PlanSequence `json:"steps,omitempty"`
	Limit    int          `json:"limit,omitempty"`
	FailFast bool         `json:"fail_fast,omitempty"`
}

func (c *InParallelConfig) UnmarshalJSON(payload []byte) error {
	var data interface{}
	err := json.Unmarshal(payload, &data)
	if err != nil {
		return err
	}

	switch actual := data.(type) {
	case []interface{}:
		if err := json.Unmarshal(payload, &c.Steps); err != nil {
			return fmt.Errorf("failed to unmarshal parallel steps: %s", err)
		}
	case map[string]interface{}:
		// Used to avoid infinite recursion when unmarshalling this variant.
		type target InParallelConfig

		var t target
		if err := json.Unmarshal(payload, &t); err != nil {
			return fmt.Errorf("failed to unmarshal parallel config: %s", err)
		}

		c.Steps, c.Limit, c.FailFast = t.Steps, t.Limit, t.FailFast
	default:
		return fmt.Errorf("wrong type for parallel config: %v", actual)
	}

	return nil
}

// A PlanConfig is a flattened set of configuration corresponding to
// a particular Plan, where Source and Version are populated lazily.
type PlanConfig struct {
	// core steps
	Get         string `json:"get,omitempty"`
	Put         string `json:"put,omitempty"`
	Task        string `json:"task,omitempty"`
	SetPipeline string `json:"set_pipeline,omitempty"`

	// step aggregators
	Do         *PlanSequence     `json:"do,omitempty"`
	Aggregate  *PlanSequence     `json:"aggregate,omitempty"`
	InParallel *InParallelConfig `json:"in_parallel,omitempty"`

	// step hooks
	OnAbort   *PlanConfig `json:"on_abort,omitempty"`
	OnError   *PlanConfig `json:"on_error,omitempty"`
	OnFailure *PlanConfig `json:"on_failure,omitempty"`
	OnSuccess *PlanConfig `json:"on_success,omitempty"`
	Ensure    *PlanConfig `json:"ensure,omitempty"`

	// step modifiers
	Try      *PlanConfig `json:"try,omitempty"`
	Timeout  string      `json:"timeout,omitempty"`
	Attempts int         `json:"attempts,omitempty"`

	// step attributes

	// with Get: jobs that this resource must have made it through
	Passed []string `json:"passed,omitempty"`

	// with Get: whether to trigger based on this resource changing
	Trigger bool `json:"trigger,omitempty"`

	// with Get/Put: fetch as the Get/Put name, using this resouce
	Resource string `json:"resource,omitempty"`

	// with Get/Put: fetch the Get/Put resource, as this name
	As string `json:"as,omitempty"`

	// with Put: artifacts to provide to the put step
	Inputs *InputsConfig `json:"inputs,omitempty"`

	// with Task: run the task privileged
	Privileged bool `json:"privileged,omitempty"`

	// with Task: path to a task config to execute
	// with SetPipeline: path to a pipeline config to set
	File string `json:"file,omitempty"`

	// with Task/SetPipeline: vars to interpolate into the config file
	Vars Params `json:"vars,omitempty"`

	// with Task: inlined task config
	TaskConfig *TaskConfig `json:"config,omitempty"`

	// with Get/Put: specifying params to the resource
	// with Task: passing params as env to task
	Params Params `json:"params,omitempty"`

	// with Put: specify params for the subsequent Get
	GetParams Params `json:"get_params,omitempty"`

	// with Task: map an artifact to a task's input
	InputMapping map[string]string `json:"input_mapping,omitempty"`

	// with Task: map a task's output to an artifact
	OutputMapping map[string]string `json:"output_mapping,omitempty"`

	// with Task: an artifact to use as the task's image; must have rootfs/ and metadata.json
	ImageArtifactName string `json:"image,omitempty"`

	// with Get/Put/Task: tags to require when selecting the worker for the step's container
	Tags Tags `json:"tags,omitempty"`

	// with Get: either 'latest' (default), 'every' (to not skip available
	// versions), or a specific version e.g. '{ref: abcdef}'
	Version *VersionConfig `json:"version,omitempty"`
}

func (config PlanConfig) Name() string {
	if config.Get != "" {
		return config.Get
	}

	if config.Put != "" {
		return config.Put
	}

	if config.Task != "" {
		return config.Task
	}

	return ""
}

func (config PlanConfig) ResourceName() string {
	resourceName := config.Resource
	if resourceName != "" {
		return resourceName
	}

	resourceName = config.Get
	if resourceName != "" {
		return resourceName
	}

	resourceName = config.Put
	if resourceName != "" {
		return resourceName
	}

	panic("no resource name!")
}

func (config PlanConfig) Hooks() Hooks {
	return Hooks{
		Abort:   config.OnAbort,
		Error:   config.OnError,
		Failure: config.OnFailure,
		Success: config.OnSuccess,
		Ensure:  config.Ensure,
	}
}

type ResourceConfigs []ResourceConfig

func (resources ResourceConfigs) Lookup(name string) (ResourceConfig, bool) {
	for _, resource := range resources {
		if resource.Name == name {
			return resource, true
		}
	}

	return ResourceConfig{}, false
}

type JobConfigs []JobConfig

func (jobs JobConfigs) Lookup(name string) (JobConfig, bool) {
	for _, job := range jobs {
		if job.Name == name {
			return job, true
		}
	}

	return JobConfig{}, false
}

func (config Config) JobIsPublic(jobName string) (bool, error) {
	job, found := config.Jobs.Lookup(jobName)
	if !found {
		return false, fmt.Errorf("cannot find job with job name '%s'", jobName)
	}

	return job.Public, nil
}

func DefaultTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,

		// https://wiki.mozilla.org/Security/Server_Side_TLS#Modern_compatibility
		CurvePreferences: []tls.CurveID{
			tls.CurveP256,
			tls.CurveP384,
			tls.CurveP521,
		},

		// Security team recommends a very restricted set of cipher suites
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},

		PreferServerCipherSuites: true,
		NextProtos:               []string{"h2"},
	}
}

func DefaultSSHConfig() ssh.Config {
	return ssh.Config{
		// use the defaults prefered by go, see https://github.com/golang/crypto/blob/master/ssh/common.go
		Ciphers: nil,

		// CIS recommends a certain set of MAC algorithms to be used in SSH connections. This restricts the set from a more permissive set used by default by Go.
		// See https://infosec.mozilla.org/guidelines/openssh.html and https://www.cisecurity.org/cis-benchmarks/
		MACs: []string{
			"hmac-sha2-256-etm@openssh.com",
			"hmac-sha2-256",
		},

		//[KEX Recommendations for SSH IETF](https://tools.ietf.org/html/draft-ietf-curdle-ssh-kex-sha2-10#section-4)
		//[Mozilla Openssh Reference](https://infosec.mozilla.org/guidelines/openssh.html)
		KeyExchanges: []string{
			"ecdh-sha2-nistp256",
			"ecdh-sha2-nistp384",
			"ecdh-sha2-nistp521",
			"curve25519-sha256@libssh.org",
		},
	}
}
