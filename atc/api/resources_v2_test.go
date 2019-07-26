package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry/bosh-cli/director/template"
	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/api/accessor/accessorfakes"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/db/dbfakes"
)

var _ = Describe("Resources V2 API", func() {
	var (
		fakePipeline *dbfakes.FakePipeline
		fakeaccess   = new(accessorfakes.FakeAccess)
		variables    creds.Variables
	)

	BeforeEach(func() {
		fakePipeline = new(dbfakes.FakePipeline)
		dbTeamFactory.FindTeamReturns(dbTeam, true, nil)
		dbTeam.PipelineReturns(fakePipeline, true, nil)
	})

	JustBeforeEach(func() {
		fakeAccessor.CreateReturns(fakeaccess)
	})

	Describe("POST /api/v2/teams/:team_name/pipelines/:pipeline_name/resources/:resource_name/check", func() {
		var checkRequestBody atc.CheckRequestBody
		var response *http.Response

		BeforeEach(func() {
			checkRequestBody = atc.CheckRequestBody{}
		})

		JustBeforeEach(func() {
			reqPayload, err := json.Marshal(checkRequestBody)
			Expect(err).NotTo(HaveOccurred())

			request, err := http.NewRequest("POST", server.URL+"/api/v2/teams/a-team/pipelines/a-pipeline/resources/resource-name/check", bytes.NewBuffer(reqPayload))
			Expect(err).NotTo(HaveOccurred())
			request.Header.Set("Content-Type", "application/json")

			response, err = client.Do(request)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when authorized", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthenticatedReturns(true)
				fakeaccess.IsAuthorizedReturns(true)
			})

			Context("when looking up the resource fails", func() {
				BeforeEach(func() {
					fakePipeline.ResourceReturns(nil, false, errors.New("nope"))
				})
				It("returns 500", func() {
					Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
				})
			})

			Context("when the resource is not found", func() {
				BeforeEach(func() {
					fakePipeline.ResourceReturns(nil, false, nil)
				})
				It("returns 404", func() {
					Expect(response.StatusCode).To(Equal(http.StatusNotFound))
				})
			})

			Context("when it finds the resource", func() {
				var fakeResource *dbfakes.FakeResource

				BeforeEach(func() {
					fakeResource = new(dbfakes.FakeResource)
					fakeResource.IDReturns(1)
					fakePipeline.ResourceReturns(fakeResource, true, nil)
				})

				Context("when looking up the resource types fails", func() {
					BeforeEach(func() {
						fakePipeline.ResourceTypesReturns(nil, errors.New("nope"))
					})
					It("returns 500", func() {
						Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
					})
				})

				Context("when looking up the resource types succeeds", func() {
					var fakeResourceTypes db.ResourceTypes

					BeforeEach(func() {
						fakeResourceTypes = db.ResourceTypes{}
						fakePipeline.ResourceTypesReturns(fakeResourceTypes, nil)
					})

					It("checks with no version specified", func() {
						Expect(fakeChecker.CheckCallCount()).To(Equal(1))
						actualResource, actualResourceTypes, actualFromVersion := fakeChecker.CheckArgsForCall(0)
						Expect(actualResource).To(Equal(fakeResource))
						Expect(actualResourceTypes).To(Equal(fakeResourceTypes))
						Expect(actualFromVersion).To(BeNil())
					})

					Context("when checking with a version specified", func() {
						BeforeEach(func() {
							checkRequestBody = atc.CheckRequestBody{
								From: atc.Version{
									"some-version-key": "some-version-value",
								},
							}
						})

						It("checks with the version specified", func() {
							Expect(fakeChecker.CheckCallCount()).To(Equal(1))
							actualResource, actualResourceTypes, actualFromVersion := fakeChecker.CheckArgsForCall(0)
							Expect(actualResource).To(Equal(fakeResource))
							Expect(actualResourceTypes).To(Equal(fakeResourceTypes))
							Expect(actualFromVersion).To(Equal(checkRequestBody.From))
						})
					})

					Context("when checking fails", func() {
						BeforeEach(func() {
							fakeChecker.CheckReturns(nil, false, errors.New("nope"))
						})

						It("returns 500", func() {
							Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
						})
					})

					Context("when checking does not create a new check", func() {
						BeforeEach(func() {
							fakeChecker.CheckReturns(nil, false, nil)
						})

						It("returns 500", func() {
							Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
						})
					})

					Context("when checking creates a new check", func() {
						var fakeCheck *dbfakes.FakeCheck

						BeforeEach(func() {
							fakeCheck = new(dbfakes.FakeCheck)
							fakeCheck.IDReturns(10)
							fakeCheck.StatusReturns("started")
							fakeCheck.CreateTimeReturns(time.Date(2000, 01, 01, 0, 0, 0, 0, time.UTC))
							fakeCheck.StartTimeReturns(time.Date(2001, 01, 01, 0, 0, 0, 0, time.UTC))
							fakeCheck.EndTimeReturns(time.Date(2002, 01, 01, 0, 0, 0, 0, time.UTC))

							fakeChecker.CheckReturns(fakeCheck, true, nil)
						})

						It("returns 201", func() {
							Expect(response.StatusCode).To(Equal(http.StatusCreated))
							Expect(ioutil.ReadAll(response.Body)).To(MatchJSON(`{
                 "id": 10,
								 "status": "started",
								 "create_time": 946684800,
								 "start_time": 978307200,
								 "end_time": 1009843200
							}`))
						})
					})
				})
			})
		})

		Context("when not authenticated", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthenticatedReturns(false)
			})

			It("returns Unauthorized", func() {
				Expect(response.StatusCode).To(Equal(http.StatusUnauthorized))
			})
		})
	})

	Describe("POST /api/v2/teams/:team_name/pipelines/:pipeline_name/resource-types/:resource_type_name/check", func() {
		var checkRequestBody atc.CheckRequestBody
		var response *http.Response

		BeforeEach(func() {
			checkRequestBody = atc.CheckRequestBody{}
		})

		JustBeforeEach(func() {
			reqPayload, err := json.Marshal(checkRequestBody)
			Expect(err).NotTo(HaveOccurred())

			request, err := http.NewRequest("POST", server.URL+"/api/v2/teams/a-team/pipelines/a-pipeline/resource-types/resource-type-name/check", bytes.NewBuffer(reqPayload))
			Expect(err).NotTo(HaveOccurred())
			request.Header.Set("Content-Type", "application/json")

			response, err = client.Do(request)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when not authenticated", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthenticatedReturns(false)
			})

			It("returns Unauthorized", func() {
				Expect(response.StatusCode).To(Equal(http.StatusUnauthorized))
			})
		})

		Context("when not authorized", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthenticatedReturns(true)
				fakeaccess.IsAuthorizedReturns(false)
			})

			It("returns Forbidden", func() {
				Expect(response.StatusCode).To(Equal(http.StatusForbidden))
			})
		})

		Context("when authenticated and authorized", func() {

			BeforeEach(func() {
				fakeaccess.IsAuthenticatedReturns(true)
				fakeaccess.IsAuthorizedReturns(true)
			})

			Context("when looking up the resource type fails", func() {
				BeforeEach(func() {
					fakePipeline.ResourceTypeReturns(nil, false, errors.New("nope"))
				})
				It("returns 500", func() {
					Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
				})
			})

			Context("when the resource type is not found", func() {
				BeforeEach(func() {
					fakePipeline.ResourceTypeReturns(nil, false, nil)
				})
				It("returns 404", func() {
					Expect(response.StatusCode).To(Equal(http.StatusNotFound))
				})
			})

			Context("when it finds the resource type", func() {
				var fakeResourceType *dbfakes.FakeResourceType

				BeforeEach(func() {
					fakeResourceType = new(dbfakes.FakeResourceType)
					fakeResourceType.IDReturns(1)
					fakePipeline.ResourceTypeReturns(fakeResourceType, true, nil)
				})

				Context("when looking up the resource types fails", func() {
					BeforeEach(func() {
						fakePipeline.ResourceTypesReturns(nil, errors.New("nope"))
					})

					It("returns 500", func() {
						Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
					})
				})

				Context("when looking up the resource types succeeds", func() {
					var fakeResourceTypes db.ResourceTypes

					BeforeEach(func() {
						fakeResourceTypes = db.ResourceTypes{}
						fakePipeline.ResourceTypesReturns(fakeResourceTypes, nil)
					})

					It("checks with no version specified", func() {
						Expect(fakeChecker.CheckCallCount()).To(Equal(1))
						actualResourceType, actualResourceTypes, actualFromVersion := fakeChecker.CheckArgsForCall(0)
						Expect(actualResourceType).To(Equal(fakeResourceType))
						Expect(actualResourceTypes).To(Equal(fakeResourceTypes))
						Expect(actualFromVersion).To(BeNil())
					})

					Context("when checking with a version specified", func() {
						BeforeEach(func() {
							checkRequestBody = atc.CheckRequestBody{
								From: atc.Version{
									"some-version-key": "some-version-value",
								},
							}
						})

						It("checks with no version specified", func() {
							Expect(fakeChecker.CheckCallCount()).To(Equal(1))
							actualResourceType, actualResourceTypes, actualFromVersion := fakeChecker.CheckArgsForCall(0)
							Expect(actualResourceType).To(Equal(fakeResourceType))
							Expect(actualResourceTypes).To(Equal(fakeResourceTypes))
							Expect(actualFromVersion).To(Equal(checkRequestBody.From))
						})
					})

					Context("when checking fails", func() {
						BeforeEach(func() {
							fakeChecker.CheckReturns(nil, false, errors.New("nope"))
						})

						It("returns 500", func() {
							Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
						})
					})

					Context("when checking does not create a new check", func() {
						BeforeEach(func() {
							fakeChecker.CheckReturns(nil, false, nil)
						})

						It("returns 500", func() {
							Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
						})
					})

					Context("when checking creates a new check", func() {
						var fakeCheck *dbfakes.FakeCheck

						BeforeEach(func() {
							fakeCheck = new(dbfakes.FakeCheck)
							fakeCheck.IDReturns(10)
							fakeCheck.StatusReturns("started")
							fakeCheck.CreateTimeReturns(time.Date(2000, 01, 01, 0, 0, 0, 0, time.UTC))
							fakeCheck.StartTimeReturns(time.Date(2001, 01, 01, 0, 0, 0, 0, time.UTC))
							fakeCheck.EndTimeReturns(time.Date(2002, 01, 01, 0, 0, 0, 0, time.UTC))

							fakeChecker.CheckReturns(fakeCheck, true, nil)
						})

						It("returns 201", func() {
							Expect(response.StatusCode).To(Equal(http.StatusCreated))
							Expect(ioutil.ReadAll(response.Body)).To(MatchJSON(`{
                 "id": 10,
								 "status": "started",
								 "create_time": 946684800,
								 "start_time": 978307200,
								 "end_time": 1009843200
							}`))
						})
					})

				})
			})
		})
	})

	Describe("POST /api/v2/teams/:team_name/pipelines/:pipeline_name/resources/:resource_name/check/webhook", func() {
		var (
			checkRequestBody atc.CheckRequestBody
			response         *http.Response
			fakeResource     *dbfakes.FakeResource
		)

		BeforeEach(func() {
			checkRequestBody = atc.CheckRequestBody{}

			fakeResource = new(dbfakes.FakeResource)
			fakeResource.NameReturns("resource-name")
			fakeResource.IDReturns(10)
		})

		JustBeforeEach(func() {
			reqPayload, err := json.Marshal(checkRequestBody)
			Expect(err).NotTo(HaveOccurred())

			request, err := http.NewRequest("POST", server.URL+"/api/v2/teams/a-team/pipelines/a-pipeline/resources/resource-name/check/webhook?webhook_token=fake-token", bytes.NewBuffer(reqPayload))
			Expect(err).NotTo(HaveOccurred())
			request.Header.Set("Content-Type", "application/json")

			response, err = client.Do(request)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when authorized", func() {
			BeforeEach(func() {
				variables = template.StaticVariables{
					"webhook-token": "fake-token",
				}
				token, err := creds.NewString(variables, "((webhook-token))").Evaluate()
				Expect(err).NotTo(HaveOccurred())
				fakeResource.WebhookTokenReturns(token)
				fakePipeline.ResourceReturns(fakeResource, true, nil)
				fakeResource.ResourceConfigIDReturns(1)
				fakeResource.ResourceConfigScopeIDReturns(2)
			})

			It("injects the proper pipelineDB", func() {
				Expect(dbTeam.PipelineCallCount()).To(Equal(1))
				resourceName := fakePipeline.ResourceArgsForCall(0)
				Expect(resourceName).To(Equal("resource-name"))
			})

			It("tries to find the resource", func() {
				Expect(fakePipeline.ResourceCallCount()).To(Equal(1))
				Expect(fakePipeline.ResourceArgsForCall(0)).To(Equal("resource-name"))
			})

			Context("when finding the resource succeeds", func() {
				BeforeEach(func() {
					fakePipeline.ResourceReturns(fakeResource, true, nil)
				})

				Context("when finding the resource types fails", func() {
					BeforeEach(func() {
						fakePipeline.ResourceTypesReturns(nil, errors.New("oops"))
					})

					It("returns 500", func() {
						Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
					})
				})

				Context("when finding the resource types succeeds", func() {
					var fakeResourceTypes db.ResourceTypes

					BeforeEach(func() {
						fakeResourceTypes = db.ResourceTypes{}
						fakePipeline.ResourceTypesReturns(fakeResourceTypes, nil)
					})

					It("checks with a nil version", func() {
						Expect(fakeChecker.CheckCallCount()).To(Equal(1))
						actualResource, actualResourceTypes, actualFromVersion := fakeChecker.CheckArgsForCall(0)
						Expect(actualResource).To(Equal(fakeResource))
						Expect(actualResourceTypes).To(Equal(fakeResourceTypes))
						Expect(actualFromVersion).To(BeNil())
					})

					Context("when checking fails", func() {
						BeforeEach(func() {
							fakeChecker.CheckReturns(nil, false, errors.New("nope"))
						})

						It("returns 500", func() {
							Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
						})
					})

					Context("when checking does not create a new check", func() {
						BeforeEach(func() {
							fakeChecker.CheckReturns(nil, false, nil)
						})

						It("returns 500", func() {
							Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
						})
					})

					Context("when checking creates a new check", func() {
						var fakeCheck *dbfakes.FakeCheck

						BeforeEach(func() {
							fakeCheck = new(dbfakes.FakeCheck)
							fakeCheck.IDReturns(10)
							fakeCheck.StatusReturns("started")
							fakeCheck.CreateTimeReturns(time.Date(2000, 01, 01, 0, 0, 0, 0, time.UTC))
							fakeCheck.StartTimeReturns(time.Date(2001, 01, 01, 0, 0, 0, 0, time.UTC))
							fakeCheck.EndTimeReturns(time.Date(2002, 01, 01, 0, 0, 0, 0, time.UTC))

							fakeChecker.CheckReturns(fakeCheck, true, nil)
						})

						It("returns 201", func() {
							Expect(response.StatusCode).To(Equal(http.StatusCreated))
							Expect(ioutil.ReadAll(response.Body)).To(MatchJSON(`{
                 "id": 10,
								 "status": "started",
								 "create_time": 946684800,
								 "start_time": 978307200,
								 "end_time": 1009843200
							}`))
						})
					})
				})
			})

			Context("when finding the resource fails", func() {
				BeforeEach(func() {
					fakePipeline.ResourceReturns(nil, false, errors.New("oops"))
				})

				It("returns 500", func() {
					Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
				})
			})

			Context("when the resource is not found", func() {
				BeforeEach(func() {
					fakePipeline.ResourceReturns(nil, false, nil)
				})

				It("returns 404", func() {
					Expect(response.StatusCode).To(Equal(http.StatusNotFound))
				})
			})
		})

		Context("when unauthorized", func() {
			BeforeEach(func() {
				fakeResource.WebhookTokenReturns("wrong-token")
				fakePipeline.ResourceReturns(fakeResource, true, nil)
			})
			It("returns 401", func() {
				Expect(response.StatusCode).To(Equal(http.StatusUnauthorized))
			})
		})
	})
})
