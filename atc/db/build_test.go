package db_test

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/event"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Build", func() {
	var (
		team db.Team
	)

	BeforeEach(func() {
		var err error
		team, err = teamFactory.CreateTeam(atc.Team{Name: "some-team"})
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Reload", func() {
		It("updates the model", func() {
			build, err := team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())
			started, err := build.Start(atc.Plan{})
			Expect(err).NotTo(HaveOccurred())
			Expect(started).To(BeTrue())

			Expect(build.Status()).To(Equal(db.BuildStatusPending))

			found, err := build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(build.Status()).To(Equal(db.BuildStatusStarted))
		})
	})

	Describe("Drain", func() {
		It("defaults drain to false in the beginning", func() {
			build, err := team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())
			Expect(build.IsDrained()).To(BeFalse())
		})

		It("has drain set to true after a drain and a reload", func() {
			build, err := team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())

			err = build.SetDrained(true)
			Expect(err).NotTo(HaveOccurred())

			drained := build.IsDrained()
			Expect(drained).To(BeTrue())

			_, err = build.Reload()
			Expect(err).NotTo(HaveOccurred())
			drained = build.IsDrained()
			Expect(drained).To(BeTrue())
		})
	})

	Describe("Start", func() {
		var err error
		var started bool
		var build db.Build
		var plan atc.Plan

		BeforeEach(func() {
			plan = atc.Plan{
				ID: atc.PlanID("56"),
				Get: &atc.GetPlan{
					Type:     "some-type",
					Name:     "some-name",
					Resource: "some-resource",
					Source:   atc.Source{"some": "source"},
					Params:   atc.Params{"some": "params"},
					Version:  &atc.Version{"some": "version"},
					Tags:     atc.Tags{"some-tags"},
					VersionedResourceTypes: atc.VersionedResourceTypes{
						{
							ResourceType: atc.ResourceType{
								Name:       "some-name",
								Source:     atc.Source{"some": "source"},
								Type:       "some-type",
								Privileged: true,
								Tags:       atc.Tags{"some-tags"},
							},
							Version: atc.Version{"some-resource-type": "version"},
						},
					},
				},
			}

			build, err = team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())
		})

		JustBeforeEach(func() {
			started, err = build.Start(plan)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("build has been aborted", func() {
			BeforeEach(func() {
				err = build.MarkAsAborted()
				Expect(err).NotTo(HaveOccurred())
			})

			It("does not start the build", func() {
				Expect(started).To(BeFalse())
			})

			It("leaves the build in pending state", func() {
				found, err := build.Reload()
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(build.Status()).To(Equal(db.BuildStatusPending))
			})
		})

		Context("build has not been aborted", func() {
			It("starts the build", func() {
				Expect(started).To(BeTrue())
			})

			It("creates Start event", func() {
				found, err := build.Reload()
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(build.Status()).To(Equal(db.BuildStatusStarted))

				events, err := build.Events(0)
				Expect(err).NotTo(HaveOccurred())

				defer db.Close(events)

				Expect(events.Next()).To(Equal(envelope(event.Status{
					Status: atc.StatusStarted,
					Time:   build.StartTime().Unix(),
				})))
			})

			It("updates build status", func() {
				found, err := build.Reload()
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(build.Status()).To(Equal(db.BuildStatusStarted))
			})

			It("saves the public plan", func() {
				found, err := build.Reload()
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(build.PublicPlan()).To(Equal(plan.Public()))
			})
		})
	})

	Describe("Finish", func() {
		var pipeline db.Pipeline
		var build db.Build
		var expectedOutputs []db.AlgorithmOutput

		BeforeEach(func() {
			setupTx, err := dbConn.Begin()
			Expect(err).ToNot(HaveOccurred())

			brt := db.BaseResourceType{
				Name: "some-type",
			}

			_, err = brt.FindOrCreate(setupTx, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(setupTx.Commit()).To(Succeed())

			pipelineConfig := atc.Config{
				Jobs: atc.JobConfigs{
					{
						Name: "some-job",
					},
				},
				Resources: atc.ResourceConfigs{
					{
						Name:   "some-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "source"},
					},
					{
						Name:   "some-other-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "other-source"},
					},
				},
			}

			pipeline, _, err = team.SavePipeline("some-pipeline", pipelineConfig, db.ConfigVersion(1), db.PipelineUnpaused)
			Expect(err).ToNot(HaveOccurred())

			job, found, err := pipeline.Job("some-job")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			resource1, found, err := pipeline.Resource("some-resource")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			resourceConfigScope1, err := resource1.SetResourceConfig(logger, atc.Source{"some": "source"}, creds.VersionedResourceTypes{})
			Expect(err).ToNot(HaveOccurred())

			err = resourceConfigScope1.SaveVersions([]atc.Version{
				{"ver": "1"},
				{"ver": "2"},
			})
			Expect(err).ToNot(HaveOccurred())

			resource2, found, err := pipeline.Resource("some-other-resource")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			resourceConfigScope2, err := resource2.SetResourceConfig(logger, atc.Source{"some": "other-source"}, creds.VersionedResourceTypes{})
			Expect(err).ToNot(HaveOccurred())

			err = resourceConfigScope2.SaveVersions([]atc.Version{
				{"ver": "1"},
				{"ver": "2"},
				{"ver": "3"},
			})
			Expect(err).ToNot(HaveOccurred())

			build, err = job.CreateBuild()
			Expect(err).NotTo(HaveOccurred())

			err = job.SaveNextInputMapping(db.InputMapping{
				"input-1": db.InputResult{
					Input: &db.AlgorithmInput{
						AlgorithmVersion: db.AlgorithmVersion{
							Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "1"})),
							ResourceID: resource1.ID(),
						},
						FirstOccurrence: true,
					},
					PassedBuildIDs: []int{},
				},
				"input-2": db.InputResult{
					Input: &db.AlgorithmInput{
						AlgorithmVersion: db.AlgorithmVersion{
							Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "3"})),
							ResourceID: resource2.ID(),
						},
						FirstOccurrence: true,
					},
					PassedBuildIDs: []int{},
				},
				"input-3": db.InputResult{
					Input: &db.AlgorithmInput{
						AlgorithmVersion: db.AlgorithmVersion{
							Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "2"})),
							ResourceID: resource1.ID(),
						},
						FirstOccurrence: true,
					},
					PassedBuildIDs: []int{},
				},
			}, true)
			Expect(err).NotTo(HaveOccurred())

			_, found, err = build.AdoptInputsAndPipes()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())

			// save explicit output from 'put'
			err = build.SaveOutput(logger, "some-type", atc.Source{"some": "source"}, creds.VersionedResourceTypes{}, atc.Version{"ver": "2"}, nil, "output-1", "some-resource")
			Expect(err).NotTo(HaveOccurred())

			// save explicit output from 'put'
			err = build.SaveOutput(logger, "some-type", atc.Source{"some": "source"}, creds.VersionedResourceTypes{}, atc.Version{"ver": "3"}, nil, "output-2", "some-resource")
			Expect(err).NotTo(HaveOccurred())

			err = build.Finish(db.BuildStatusSucceeded)
			Expect(err).NotTo(HaveOccurred())

			expectedOutputs = []db.AlgorithmOutput{
				{
					AlgorithmVersion: db.AlgorithmVersion{
						Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "1"})),
						ResourceID: resource1.ID(),
					},
					InputName: "input-1",
				},
				{
					AlgorithmVersion: db.AlgorithmVersion{
						Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "3"})),
						ResourceID: resource2.ID(),
					},
					InputName: "input-2",
				},
				{
					AlgorithmVersion: db.AlgorithmVersion{
						Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "2"})),
						ResourceID: resource1.ID(),
					},
					InputName: "input-3",
				},
				{
					AlgorithmVersion: db.AlgorithmVersion{
						Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "2"})),
						ResourceID: resource1.ID(),
					},
					InputName: "output-1",
				},
				{
					AlgorithmVersion: db.AlgorithmVersion{
						Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "3"})),
						ResourceID: resource1.ID(),
					},
					InputName: "output-2",
				},
			}
		})

		It("creates Finish event", func() {
			found, err := build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(build.Status()).To(Equal(db.BuildStatusSucceeded))

			events, err := build.Events(0)
			Expect(err).NotTo(HaveOccurred())

			defer db.Close(events)

			Expect(events.Next()).To(Equal(envelope(event.Status{
				Status: atc.StatusSucceeded,
				Time:   build.EndTime().Unix(),
			})))
		})

		It("updates build status", func() {
			found, err := build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(build.Status()).To(Equal(db.BuildStatusSucceeded))
		})

		It("clears out the private plan", func() {
			found, err := build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(build.PrivatePlan()).To(Equal(atc.Plan{}))
		})

		It("sets completed to true", func() {
			Expect(build.IsCompleted()).To(BeFalse())
			Expect(build.IsRunning()).To(BeTrue())

			found, err := build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(build.IsCompleted()).To(BeTrue())
			Expect(build.IsRunning()).To(BeFalse())
		})

		It("inserts inputs and outputs into successful build versions", func() {
			versionsDB, err := pipeline.LoadVersionsDB()
			Expect(err).NotTo(HaveOccurred())

			outputs, err := versionsDB.SuccessfulBuildOutputs(build.ID())
			Expect(err).NotTo(HaveOccurred())
			Expect(outputs).To(ConsistOf(expectedOutputs))
		})
	})

	Describe("Abort", func() {
		var build db.Build
		BeforeEach(func() {
			var err error
			build, err = team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())

			err = build.MarkAsAborted()
			Expect(err).NotTo(HaveOccurred())
		})

		It("updates aborted to true", func() {
			found, err := build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(build.IsAborted()).To(BeTrue())
		})
	})

	Describe("Events", func() {
		It("saves and emits status events", func() {
			build, err := team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())

			By("allowing you to subscribe when no events have yet occurred")
			events, err := build.Events(0)
			Expect(err).NotTo(HaveOccurred())

			defer db.Close(events)

			By("emitting a status event when started")
			started, err := build.Start(atc.Plan{})
			Expect(err).NotTo(HaveOccurred())
			Expect(started).To(BeTrue())

			found, err := build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())

			Expect(events.Next()).To(Equal(envelope(event.Status{
				Status: atc.StatusStarted,
				Time:   build.StartTime().Unix(),
			})))

			By("emitting a status event when finished")
			err = build.Finish(db.BuildStatusSucceeded)
			Expect(err).NotTo(HaveOccurred())

			found, err = build.Reload()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())

			Expect(events.Next()).To(Equal(envelope(event.Status{
				Status: atc.StatusSucceeded,
				Time:   build.EndTime().Unix(),
			})))

			By("ending the stream when finished")
			_, err = events.Next()
			Expect(err).To(Equal(db.ErrEndOfBuildEventStream))
		})
	})

	Describe("SaveEvent", func() {
		It("saves and propagates events correctly", func() {
			build, err := team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())

			By("allowing you to subscribe when no events have yet occurred")
			events, err := build.Events(0)
			Expect(err).NotTo(HaveOccurred())

			defer db.Close(events)

			By("saving them in order")
			err = build.SaveEvent(event.Log{
				Payload: "some ",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(events.Next()).To(Equal(envelope(event.Log{
				Payload: "some ",
			})))

			err = build.SaveEvent(event.Log{
				Payload: "log",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(events.Next()).To(Equal(envelope(event.Log{
				Payload: "log",
			})))

			By("allowing you to subscribe from an offset")
			eventsFrom1, err := build.Events(1)
			Expect(err).NotTo(HaveOccurred())

			defer db.Close(eventsFrom1)

			Expect(eventsFrom1.Next()).To(Equal(envelope(event.Log{
				Payload: "log",
			})))

			By("notifying those waiting on events as soon as they're saved")
			nextEvent := make(chan event.Envelope)
			nextErr := make(chan error)

			go func() {
				event, err := events.Next()
				if err != nil {
					nextErr <- err
				} else {
					nextEvent <- event
				}
			}()

			Consistently(nextEvent).ShouldNot(Receive())
			Consistently(nextErr).ShouldNot(Receive())

			err = build.SaveEvent(event.Log{
				Payload: "log 2",
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(nextEvent).Should(Receive(Equal(envelope(event.Log{
				Payload: "log 2",
			}))))

			By("returning ErrBuildEventStreamClosed for Next calls after Close")
			events3, err := build.Events(0)
			Expect(err).NotTo(HaveOccurred())

			err = events3.Close()
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				_, err := events3.Next()
				return err
			}).Should(Equal(db.ErrBuildEventStreamClosed))
		})
	})

	Describe("SaveOutput", func() {
		var pipeline db.Pipeline
		var job db.Job
		var resourceConfigScope db.ResourceConfigScope

		BeforeEach(func() {
			pipelineConfig := atc.Config{
				Jobs: atc.JobConfigs{
					{
						Name: "some-job",
					},
				},
				Resources: atc.ResourceConfigs{
					{
						Name: "some-implicit-resource",
						Type: "some-type",
					},
					{
						Name:   "some-explicit-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "explicit-source"},
					},
				},
			}

			var err error
			pipeline, _, err = team.SavePipeline("some-pipeline", pipelineConfig, db.ConfigVersion(1), db.PipelineUnpaused)
			Expect(err).ToNot(HaveOccurred())

			var found bool
			job, found, err = pipeline.Job("some-job")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			setupTx, err := dbConn.Begin()
			Expect(err).ToNot(HaveOccurred())

			brt := db.BaseResourceType{
				Name: "some-type",
			}

			_, err = brt.FindOrCreate(setupTx, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(setupTx.Commit()).To(Succeed())

			resource, found, err := pipeline.Resource("some-explicit-resource")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			resourceConfigScope, err = resource.SetResourceConfig(logger, atc.Source{"some": "explicit-source"}, creds.VersionedResourceTypes{})
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when the version does not exist", func() {
			It("can save a build's output", func() {
				build, err := job.CreateBuild()
				Expect(err).ToNot(HaveOccurred())

				err = build.SaveOutput(logger, "some-type", atc.Source{"some": "explicit-source"}, creds.VersionedResourceTypes{}, atc.Version{"some": "version"}, []db.ResourceConfigMetadataField{
					{
						Name:  "meta1",
						Value: "data1",
					},
					{
						Name:  "meta2",
						Value: "data2",
					},
				}, "output-name", "some-explicit-resource")
				Expect(err).ToNot(HaveOccurred())

				rcv, found, err := resourceConfigScope.FindVersion(atc.Version{"some": "version"})
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())

				_, buildOutputs, err := build.Resources()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(buildOutputs)).To(Equal(1))
				Expect(buildOutputs[0].Name).To(Equal("output-name"))
				Expect(buildOutputs[0].Version).To(Equal(atc.Version(rcv.Version())))
			})
		})

		Context("when the version already exists", func() {
			var rcv db.ResourceConfigVersion

			BeforeEach(func() {
				err := resourceConfigScope.SaveVersions([]atc.Version{{"some": "version"}})
				Expect(err).ToNot(HaveOccurred())

				var found bool
				rcv, found, err = resourceConfigScope.FindVersion(atc.Version{"some": "version"})
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())
			})

			It("does not increment the check order", func() {
				build, err := job.CreateBuild()
				Expect(err).ToNot(HaveOccurred())

				err = build.SaveOutput(logger, "some-type", atc.Source{"some": "explicit-source"}, creds.VersionedResourceTypes{}, atc.Version{"some": "version"}, []db.ResourceConfigMetadataField{
					{
						Name:  "meta1",
						Value: "data1",
					},
					{
						Name:  "meta2",
						Value: "data2",
					},
				}, "output-name", "some-explicit-resource")
				Expect(err).ToNot(HaveOccurred())

				newRCV, found, err := resourceConfigScope.FindVersion(atc.Version{"some": "version"})
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())

				Expect(newRCV.CheckOrder()).To(Equal(rcv.CheckOrder()))
			})
		})
	})

	Describe("Resources", func() {
		var (
			pipeline             db.Pipeline
			job                  db.Job
			resourceConfigScope1 db.ResourceConfigScope
			resource1            db.Resource
			found                bool
		)

		BeforeEach(func() {
			setupTx, err := dbConn.Begin()
			Expect(err).ToNot(HaveOccurred())

			brt := db.BaseResourceType{
				Name: "some-type",
			}

			_, err = brt.FindOrCreate(setupTx, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(setupTx.Commit()).To(Succeed())

			pipelineConfig := atc.Config{
				Jobs: atc.JobConfigs{
					{
						Name: "some-job",
					},
				},
				Resources: atc.ResourceConfigs{
					{
						Name:   "some-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "source"},
					},
					{
						Name:   "some-other-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "source-2"},
					},
					{
						Name:   "some-unused-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "source-3"},
					},
				},
			}

			pipeline, _, err = team.SavePipeline("some-pipeline", pipelineConfig, db.ConfigVersion(1), db.PipelineUnpaused)
			Expect(err).ToNot(HaveOccurred())

			job, found, err = pipeline.Job("some-job")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			resource1, found, err = pipeline.Resource("some-resource")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			resource2, found, err := pipeline.Resource("some-other-resource")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			resourceConfigScope1, err = resource1.SetResourceConfig(logger, atc.Source{"some": "source-1"}, creds.VersionedResourceTypes{})
			Expect(err).ToNot(HaveOccurred())

			_, err = resource2.SetResourceConfig(logger, atc.Source{"some": "source-2"}, creds.VersionedResourceTypes{})
			Expect(err).ToNot(HaveOccurred())

			err = resourceConfigScope1.SaveVersions([]atc.Version{
				{"ver": "1"},
				{"ver": "2"},
			})
			Expect(err).ToNot(HaveOccurred())

			// This version should not be returned by the Resources method because it has a check order of 0
			created, err := resource1.SaveUncheckedVersion(atc.Version{"ver": "not-returned"}, nil, resourceConfigScope1.ResourceConfig(), creds.VersionedResourceTypes{})
			Expect(err).ToNot(HaveOccurred())
			Expect(created).To(BeTrue())
		})

		It("returns build inputs and outputs", func() {
			build, err := job.CreateBuild()
			Expect(err).NotTo(HaveOccurred())

			// save a normal 'get'
			err = job.SaveNextInputMapping(db.InputMapping{
				"some-input": db.InputResult{
					Input: &db.AlgorithmInput{
						AlgorithmVersion: db.AlgorithmVersion{
							Version:    db.ResourceVersion(convertToMD5(atc.Version{"ver": "1"})),
							ResourceID: resource1.ID(),
						},
						FirstOccurrence: true,
					},
					PassedBuildIDs: []int{},
				},
			}, true)
			Expect(err).NotTo(HaveOccurred())

			_, found, err := build.AdoptInputsAndPipes()
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())

			// save explicit output from 'put'
			err = build.SaveOutput(logger, "some-type", atc.Source{"some": "source-2"}, creds.VersionedResourceTypes{}, atc.Version{"ver": "2"}, nil, "some-output-name", "some-other-resource")
			Expect(err).NotTo(HaveOccurred())

			inputs, outputs, err := build.Resources()
			Expect(err).NotTo(HaveOccurred())

			Expect(inputs).To(ConsistOf([]db.BuildInput{
				{Name: "some-input", Version: atc.Version{"ver": "1"}, ResourceID: resource1.ID(), FirstOccurrence: true},
			}))

			Expect(outputs).To(ConsistOf([]db.BuildOutput{
				{
					Name:    "some-output-name",
					Version: atc.Version{"ver": "2"},
				},
			}))
		})

		It("can't get no satisfaction (resources from a one-off build)", func() {
			oneOffBuild, err := team.CreateOneOffBuild()
			Expect(err).NotTo(HaveOccurred())

			inputs, outputs, err := oneOffBuild.Resources()
			Expect(err).NotTo(HaveOccurred())

			Expect(inputs).To(BeEmpty())
			Expect(outputs).To(BeEmpty())
		})
	})

	Describe("Pipeline", func() {
		var (
			build           db.Build
			foundPipeline   db.Pipeline
			createdPipeline db.Pipeline
			found           bool
		)

		JustBeforeEach(func() {
			var err error
			foundPipeline, found, err = build.Pipeline()
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when a job build", func() {
			BeforeEach(func() {
				var err error
				createdPipeline, _, err = team.SavePipeline("some-pipeline", atc.Config{
					Jobs: atc.JobConfigs{
						{
							Name: "some-job",
						},
					},
				}, db.ConfigVersion(1), db.PipelineUnpaused)
				Expect(err).ToNot(HaveOccurred())

				job, found, err := createdPipeline.Job("some-job")
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())

				build, err = job.CreateBuild()
				Expect(err).ToNot(HaveOccurred())
			})

			It("returns the correct pipeline", func() {
				Expect(found).To(BeTrue())
				Expect(foundPipeline).To(Equal(createdPipeline))
			})
		})

		Context("when a one off build", func() {
			BeforeEach(func() {
				var err error
				build, err = team.CreateOneOffBuild()
				Expect(err).ToNot(HaveOccurred())
			})

			It("does not return a pipeline", func() {
				Expect(found).To(BeFalse())
				Expect(foundPipeline).To(BeNil())
			})
		})
	})

	Describe("Preparation", func() {
		var (
			build             db.Build
			err               error
			expectedBuildPrep db.BuildPreparation
		)
		BeforeEach(func() {
			expectedBuildPrep = db.BuildPreparation{
				BuildID:             123456789,
				PausedPipeline:      db.BuildPreparationStatusNotBlocking,
				PausedJob:           db.BuildPreparationStatusNotBlocking,
				MaxRunningBuilds:    db.BuildPreparationStatusNotBlocking,
				Inputs:              map[string]db.BuildPreparationStatus{},
				InputsSatisfied:     db.BuildPreparationStatusNotBlocking,
				MissingInputReasons: db.MissingInputReasons{},
			}
		})

		Context("for one-off build", func() {
			BeforeEach(func() {
				build, err = team.CreateOneOffBuild()
				Expect(err).NotTo(HaveOccurred())

				expectedBuildPrep.BuildID = build.ID()
			})

			It("returns build preparation", func() {
				buildPrep, found, err := build.Preparation()
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(buildPrep).To(Equal(expectedBuildPrep))
			})

			Context("when the build is started", func() {
				BeforeEach(func() {
					started, err := build.Start(atc.Plan{})
					Expect(started).To(BeTrue())
					Expect(err).NotTo(HaveOccurred())

					stillExists, err := build.Reload()
					Expect(stillExists).To(BeTrue())
					Expect(err).NotTo(HaveOccurred())
				})

				It("returns build preparation", func() {
					buildPrep, found, err := build.Preparation()
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(buildPrep).To(Equal(expectedBuildPrep))
				})
			})
		})

		Context("for job build", func() {
			var (
				pipeline db.Pipeline
				job      db.Job
			)

			BeforeEach(func() {
				var err error
				pipeline, _, err = team.SavePipeline("some-pipeline", atc.Config{
					Resources: atc.ResourceConfigs{
						{
							Name: "some-resource",
							Type: "some-type",
							Source: atc.Source{
								"source-config": "some-value",
							},
						},
					},
					Jobs: atc.JobConfigs{
						{
							Name: "some-job",
							Plan: atc.PlanSequence{
								{
									Get: "some-input",
								},
							},
						},
					},
				}, db.ConfigVersion(1), db.PipelineUnpaused)
				Expect(err).ToNot(HaveOccurred())

				var found bool
				job, found, err = pipeline.Job("some-job")
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())

				build, err = job.CreateBuild()
				Expect(err).NotTo(HaveOccurred())

				expectedBuildPrep.BuildID = build.ID()

				job, found, err = pipeline.Job("some-job")
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
			})

			Context("when inputs are satisfied", func() {
				var resource db.Resource
				var rcv db.ResourceConfigVersion

				BeforeEach(func() {
					setupTx, err := dbConn.Begin()
					Expect(err).ToNot(HaveOccurred())

					brt := db.BaseResourceType{
						Name: "some-type",
					}

					_, err = brt.FindOrCreate(setupTx, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(setupTx.Commit()).To(Succeed())

					var found bool
					resource, found, err = pipeline.Resource("some-resource")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())

					resourceConfigScope, err := resource.SetResourceConfig(logger, atc.Source{"some": "source"}, creds.VersionedResourceTypes{})
					Expect(err).NotTo(HaveOccurred())

					err = resourceConfigScope.SaveVersions([]atc.Version{{"version": "v5"}})
					Expect(err).NotTo(HaveOccurred())

					rcv, found, err = resourceConfigScope.FindVersion(atc.Version{"version": "v5"})
					Expect(found).To(BeTrue())
					Expect(err).NotTo(HaveOccurred())

					err = job.SaveNextInputMapping(db.InputMapping{
						"some-input": db.InputResult{
							Input: &db.AlgorithmInput{
								AlgorithmVersion: db.AlgorithmVersion{
									Version:    db.ResourceVersion(convertToMD5(atc.Version(rcv.Version()))),
									ResourceID: resource.ID(),
								},
								FirstOccurrence: true,
							},
							PassedBuildIDs: []int{},
						},
					}, true)
					Expect(err).NotTo(HaveOccurred())

					expectedBuildPrep.Inputs = map[string]db.BuildPreparationStatus{
						"some-input": db.BuildPreparationStatusNotBlocking,
					}
				})

				Context("when the build is started", func() {
					BeforeEach(func() {
						started, err := build.Start(atc.Plan{})
						Expect(started).To(BeTrue())
						Expect(err).NotTo(HaveOccurred())

						stillExists, err := build.Reload()
						Expect(stillExists).To(BeTrue())
						Expect(err).NotTo(HaveOccurred())

						expectedBuildPrep.Inputs = map[string]db.BuildPreparationStatus{}
					})

					It("returns build preparation", func() {
						buildPrep, found, err := build.Preparation()
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())
						Expect(buildPrep).To(Equal(expectedBuildPrep))
					})
				})

				Context("when pipeline is paused", func() {
					BeforeEach(func() {
						err := pipeline.Pause()
						Expect(err).NotTo(HaveOccurred())

						expectedBuildPrep.PausedPipeline = db.BuildPreparationStatusBlocking
					})

					It("returns build preparation with paused pipeline", func() {
						buildPrep, found, err := build.Preparation()
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())
						Expect(buildPrep).To(Equal(expectedBuildPrep))
					})
				})

				Context("when job is paused", func() {
					BeforeEach(func() {
						err := job.Pause()
						Expect(err).NotTo(HaveOccurred())

						expectedBuildPrep.PausedJob = db.BuildPreparationStatusBlocking
					})

					It("returns build preparation with paused pipeline", func() {
						buildPrep, found, err := build.Preparation()
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())
						Expect(buildPrep).To(Equal(expectedBuildPrep))
					})
				})

				Context("when max running builds is reached", func() {
					BeforeEach(func() {
						var found bool
						var err error
						job, found, err = pipeline.Job("some-job")
						Expect(err).ToNot(HaveOccurred())
						Expect(found).To(BeTrue())

						newBuild, err := job.CreateBuild()
						Expect(err).NotTo(HaveOccurred())

						err = job.SaveNextInputMapping(nil, true)
						Expect(err).NotTo(HaveOccurred())

						scheduled, err := job.ScheduleBuild(newBuild)
						Expect(err).ToNot(HaveOccurred())
						Expect(scheduled).To(BeTrue())

						pipeline, _, err = team.SavePipeline("some-pipeline", atc.Config{
							Resources: atc.ResourceConfigs{
								{
									Name: "some-resource",
									Type: "some-type",
									Source: atc.Source{
										"source-config": "some-value",
									},
								},
							},
							Jobs: atc.JobConfigs{
								{
									Name:           "some-job",
									RawMaxInFlight: 1,
									Plan: atc.PlanSequence{
										{
											Get: "some-input",
										},
									},
								},
							},
						}, db.ConfigVersion(2), db.PipelineUnpaused)
						Expect(err).ToNot(HaveOccurred())

						job, found, err = pipeline.Job("some-job")
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())

						err = job.SaveNextInputMapping(db.InputMapping{
							"some-input": db.InputResult{
								Input: &db.AlgorithmInput{
									AlgorithmVersion: db.AlgorithmVersion{
										Version:    db.ResourceVersion(convertToMD5(atc.Version(rcv.Version()))),
										ResourceID: resource.ID(),
									},
									FirstOccurrence: true,
								},
								PassedBuildIDs: []int{},
							},
						}, true)
						Expect(err).NotTo(HaveOccurred())

						scheduled, err = job.ScheduleBuild(build)
						Expect(err).ToNot(HaveOccurred())
						Expect(scheduled).To(BeFalse())

						expectedBuildPrep.MaxRunningBuilds = db.BuildPreparationStatusBlocking
					})

					It("returns build preparation with max in flight reached", func() {
						buildPrep, found, err := build.Preparation()
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())
						Expect(buildPrep).To(Equal(expectedBuildPrep))
					})
				})

				Context("when max running builds is de-reached", func() {
					BeforeEach(func() {
						var err error
						pipeline, _, err = team.SavePipeline("some-pipeline", atc.Config{
							Resources: atc.ResourceConfigs{
								{
									Name: "some-resource",
									Type: "some-type",
									Source: atc.Source{
										"source-config": "some-value",
									},
								},
							},
							Jobs: atc.JobConfigs{
								{
									Name:           "some-job",
									RawMaxInFlight: 1,
									Plan: atc.PlanSequence{
										{
											Get: "some-input",
										},
									},
								},
							},
						}, db.ConfigVersion(2), db.PipelineUnpaused)
						Expect(err).ToNot(HaveOccurred())

						var found bool
						job, found, err = pipeline.Job("some-job")
						Expect(err).ToNot(HaveOccurred())
						Expect(found).To(BeTrue())

						newBuild, err := job.CreateBuild()
						Expect(err).NotTo(HaveOccurred())

						scheduled, err := job.ScheduleBuild(build)
						Expect(err).ToNot(HaveOccurred())
						Expect(scheduled).To(BeTrue())

						job, found, err = pipeline.Job("some-job")
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())

						err = newBuild.Finish(db.BuildStatusSucceeded)
						Expect(err).NotTo(HaveOccurred())
					})

					It("returns build preparation with max in flight not reached", func() {
						buildPrep, found, err := build.Preparation()
						Expect(err).NotTo(HaveOccurred())
						Expect(found).To(BeTrue())
						Expect(buildPrep).To(Equal(expectedBuildPrep))
					})
				})
			})

			Context("when inputs are not satisfied", func() {
				BeforeEach(func() {
					expectedBuildPrep.InputsSatisfied = db.BuildPreparationStatusBlocking
					expectedBuildPrep.MissingInputReasons = map[string]string{"some-input": db.MissingBuildInput}
					expectedBuildPrep.Inputs = map[string]db.BuildPreparationStatus{"some-input": db.BuildPreparationStatusBlocking}
				})

				It("returns blocking inputs satisfied", func() {
					buildPrep, found, err := build.Preparation()
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(buildPrep).To(Equal(expectedBuildPrep))
				})
			})

			Context("when some inputs are not satisfied", func() {
				BeforeEach(func() {
					pipelineConfig := atc.Config{
						Jobs: atc.JobConfigs{
							{
								Name: "some-job",
								Plan: atc.PlanSequence{
									{
										Get:     "input1",
										Version: &atc.VersionConfig{Pinned: atc.Version{"version": "v1"}},
									},
									{Get: "input2"},
									{Get: "input3", Passed: []string{"some-upstream-job"}},
									{Get: "input4"},
								},
							},
						},
						Resources: atc.ResourceConfigs{
							{Name: "input1", Type: "some-type", Source: atc.Source{"some": "source-1"}},
							{Name: "input2", Type: "some-type", Source: atc.Source{"some": "source-2"}},
							{Name: "input3", Type: "some-type", Source: atc.Source{"some": "source-3"}},
							{Name: "input4", Type: "some-type", Source: atc.Source{"some": "source-4"}},
						},
					}

					pipeline, _, err = team.SavePipeline("some-pipeline", pipelineConfig, db.ConfigVersion(2), db.PipelineUnpaused)
					Expect(err).ToNot(HaveOccurred())

					setupTx, err := dbConn.Begin()
					Expect(err).ToNot(HaveOccurred())

					brt := db.BaseResourceType{
						Name: "some-type",
					}

					_, err = brt.FindOrCreate(setupTx, false)
					Expect(err).NotTo(HaveOccurred())
					Expect(setupTx.Commit()).To(Succeed())

					job, found, err := pipeline.Job("some-job")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())

					resource1, found, err := pipeline.Resource("input1")
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())

					resourceConfig1, err := resource1.SetResourceConfig(logger, atc.Source{"some": "source-1"}, creds.VersionedResourceTypes{})
					Expect(err).NotTo(HaveOccurred())

					err = resourceConfig1.SaveVersions([]atc.Version{{"version": "v1"}})
					Expect(err).NotTo(HaveOccurred())

					versions, _, found, err := resource1.Versions(db.Page{Limit: 1})
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(versions).To(HaveLen(1))

					err = job.SaveNextInputMapping(db.InputMapping{
						"input1": db.InputResult{
							Input: &db.AlgorithmInput{
								AlgorithmVersion: db.AlgorithmVersion{
									Version: db.ResourceVersion(convertToMD5(versions[0].Version)), ResourceID: resource1.ID()},
								FirstOccurrence: true,
							},
							PassedBuildIDs: []int{},
						},
						"input2": db.InputResult{
							ResolveError: errors.New("resolve error"),
						},
						"input3": db.InputResult{
							ResolveSkipped: true,
						},
					}, false)
					Expect(err).NotTo(HaveOccurred())

					expectedBuildPrep.Inputs = map[string]db.BuildPreparationStatus{
						"input1": db.BuildPreparationStatusNotBlocking,
						"input2": db.BuildPreparationStatusBlocking,
						"input3": db.BuildPreparationStatusSkipped,
						"input4": db.BuildPreparationStatusBlocking,
					}
					expectedBuildPrep.InputsSatisfied = db.BuildPreparationStatusBlocking
					expectedBuildPrep.MissingInputReasons = db.MissingInputReasons{
						"input2": "resolve error",
						"input4": db.MissingBuildInput,
					}
				})

				It("returns blocking inputs satisfied", func() {
					buildPrep, found, err := build.Preparation()
					Expect(err).NotTo(HaveOccurred())
					Expect(found).To(BeTrue())
					Expect(buildPrep).To(Equal(expectedBuildPrep))
				})
			})
		})
	})

	Describe("AdoptInputsAndPipes", func() {
		var build, otherBuild, otherBuild2 db.Build
		var pipeline db.Pipeline
		var job, otherJob db.Job
		var buildInputs, expectedBuildInputs []db.BuildInput
		var adoptFound, reloadFound bool
		var err error

		BeforeEach(func() {
			pipelineConfig := atc.Config{
				Jobs: atc.JobConfigs{
					{
						Name: "some-job",
					},
					{
						Name: "some-other-job",
					},
				},
				Resources: atc.ResourceConfigs{
					{
						Name:   "some-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "source"},
					},
					{
						Name:   "some-other-resource",
						Type:   "some-type",
						Source: atc.Source{"some": "other-source"},
					},
				},
			}

			var err error
			pipeline, _, err = team.SavePipeline("some-pipeline", pipelineConfig, db.ConfigVersion(1), db.PipelineUnpaused)
			Expect(err).ToNot(HaveOccurred())

			var found bool
			job, found, err = pipeline.Job("some-job")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			build, err = job.CreateBuild()
			Expect(err).ToNot(HaveOccurred())

			otherJob, found, err = pipeline.Job("some-other-job")
			Expect(err).ToNot(HaveOccurred())
			Expect(found).To(BeTrue())

			otherBuild, err = otherJob.CreateBuild()
			Expect(err).ToNot(HaveOccurred())

			otherBuild2, err = otherJob.CreateBuild()
			Expect(err).ToNot(HaveOccurred())
		})

		JustBeforeEach(func() {
			buildInputs, adoptFound, err = build.AdoptInputsAndPipes()
			Expect(err).ToNot(HaveOccurred())

			reloadFound, err = build.Reload()
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when inputs are determined", func() {
			BeforeEach(func() {
				setupTx, err := dbConn.Begin()
				Expect(err).ToNot(HaveOccurred())

				brt := db.BaseResourceType{
					Name: "some-type",
				}

				_, err = brt.FindOrCreate(setupTx, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(setupTx.Commit()).To(Succeed())

				resource, found, err := pipeline.Resource("some-resource")
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())

				resourceConfig, err := resource.SetResourceConfig(logger, atc.Source{"some": "source"}, creds.VersionedResourceTypes{})
				Expect(err).ToNot(HaveOccurred())

				err = resourceConfig.SaveVersions([]atc.Version{
					{"version": "v1"},
					{"version": "v2"},
					{"version": "v3"},
				})
				Expect(err).NotTo(HaveOccurred())

				otherResource, found, err := pipeline.Resource("some-other-resource")
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())

				otherResourceConfig, err := otherResource.SetResourceConfig(logger, atc.Source{"some": "other-source"}, creds.VersionedResourceTypes{})
				Expect(err).ToNot(HaveOccurred())

				err = otherResourceConfig.SaveVersions([]atc.Version{atc.Version{"version": "v1"}})
				Expect(err).ToNot(HaveOccurred())

				versions, _, found, err := resource.Versions(db.Page{Limit: 3})
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())

				otherVersions, _, found, err := otherResource.Versions(db.Page{Limit: 3})
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())

				// Set up existing build inputs
				err = job.SaveNextInputMapping(db.InputMapping{
					"some-input-0": db.InputResult{
						Input: &db.AlgorithmInput{
							AlgorithmVersion: db.AlgorithmVersion{
								Version:    db.ResourceVersion(convertToMD5(versions[2].Version)),
								ResourceID: resource.ID(),
							},
							FirstOccurrence: false,
						},
						PassedBuildIDs: []int{otherBuild2.ID()},
					}}, true)
				Expect(err).ToNot(HaveOccurred())

				_, found, err = build.AdoptInputsAndPipes()
				Expect(err).ToNot(HaveOccurred())
				Expect(found).To(BeTrue())

				// Set up new next build inputs
				inputVersions := db.InputMapping{
					"some-input-1": db.InputResult{
						Input: &db.AlgorithmInput{
							AlgorithmVersion: db.AlgorithmVersion{
								Version:    db.ResourceVersion(convertToMD5(versions[0].Version)),
								ResourceID: resource.ID(),
							},
							FirstOccurrence: false,
						},
						PassedBuildIDs: []int{otherBuild.ID()},
					},
					"some-input-2": db.InputResult{
						Input: &db.AlgorithmInput{
							AlgorithmVersion: db.AlgorithmVersion{
								Version:    db.ResourceVersion(convertToMD5(versions[1].Version)),
								ResourceID: resource.ID(),
							},
							FirstOccurrence: false,
						},
						PassedBuildIDs: []int{},
					},
					"some-input-3": db.InputResult{
						Input: &db.AlgorithmInput{
							AlgorithmVersion: db.AlgorithmVersion{
								Version:    db.ResourceVersion(convertToMD5(otherVersions[0].Version)),
								ResourceID: otherResource.ID(),
							},
							FirstOccurrence: true,
						},
						PassedBuildIDs: []int{otherBuild.ID()},
					},
				}

				err = job.SaveNextInputMapping(inputVersions, true)
				Expect(err).ToNot(HaveOccurred())

				expectedBuildInputs = []db.BuildInput{
					{
						Name:            "some-input-1",
						ResourceID:      resource.ID(),
						Version:         versions[0].Version,
						FirstOccurrence: false,
					},
					{
						Name:            "some-input-2",
						ResourceID:      resource.ID(),
						Version:         versions[1].Version,
						FirstOccurrence: false,
					},
					{
						Name:            "some-input-3",
						ResourceID:      otherResource.ID(),
						Version:         otherVersions[0].Version,
						FirstOccurrence: true,
					},
				}
			})

			It("deletes existing build inputs and moves next build inputs to build inputs and next build pipes to build pipes", func() {
				Expect(adoptFound).To(BeTrue())
				Expect(reloadFound).To(BeTrue())

				Expect(buildInputs).To(ConsistOf(expectedBuildInputs))

				versionsDB, err := pipeline.LoadVersionsDB()
				Expect(err).ToNot(HaveOccurred())

				passedJobs := map[int]bool{otherJob.ID(): true}
				buildPipes, err := versionsDB.LatestBuildPipes(build.ID(), passedJobs)
				Expect(err).ToNot(HaveOccurred())
				Expect(buildPipes[otherJob.ID()]).To(Equal(otherBuild.ID()))
			})
		})

		Context("when inputs are not determined", func() {
			BeforeEach(func() {
				err := job.SaveNextInputMapping(db.InputMapping{
					"some-input-1": db.InputResult{
						ResolveError: errors.New("errored"),
					}}, false)
				Expect(err).ToNot(HaveOccurred())
			})

			It("does not move build inputs and pipes", func() {
				Expect(adoptFound).To(BeFalse())
				Expect(reloadFound).To(BeTrue())

				Expect(buildInputs).To(BeNil())

				versionsDB, err := pipeline.LoadVersionsDB()
				Expect(err).ToNot(HaveOccurred())

				passedJobs := map[int]bool{otherJob.ID(): true}
				buildPipes, err := versionsDB.LatestBuildPipes(build.ID(), passedJobs)
				Expect(err).ToNot(HaveOccurred())
				Expect(buildPipes).To(HaveLen(0))
			})
		})
	})
})

func envelope(ev atc.Event) event.Envelope {
	payload, err := json.Marshal(ev)
	Expect(err).ToNot(HaveOccurred())

	data := json.RawMessage(payload)

	return event.Envelope{
		Event:   ev.EventType(),
		Version: ev.Version(),
		Data:    &data,
	}
}

func convertToMD5(version atc.Version) string {
	versionJSON, err := json.Marshal(version)
	Expect(err).ToNot(HaveOccurred())

	hasher := md5.New()
	hasher.Write([]byte(versionJSON))
	return hex.EncodeToString(hasher.Sum(nil))
}
