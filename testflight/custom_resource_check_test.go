package testflight_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("When a resource type depends on another resource type", func() {
	BeforeEach(func() {
		setAndUnpausePipeline("fixtures/recursive-resource-checking.yml")
	})

	It("errors when parent has no version", func() {
		check := spawnFly("check-resource-experimental", "-r", inPipeline("recursive-custom-resource"), "-w")
		<-check.Exited
		Expect(check).To(gexec.Exit(1))
		Expect(check.Err).To(gbytes.Say("parent type has no version"))
	})

	It("can be checked in order", func() {
		check := fly("check-resource-type-experimental", "-r", inPipeline("mock-resource-parent"), "-w")
		Expect(check).To(gbytes.Say("succeeded"))

		check = fly("check-resource-type-experimental", "-r", inPipeline("mock-resource-child"), "-w")
		Expect(check).To(gbytes.Say("succeeded"))

		check = fly("check-resource-type-experimental", "-r", inPipeline("mock-resource-grandchild"), "-w")
		Expect(check).To(gbytes.Say("succeeded"))

		check = fly("check-resource-experimental", "-r", inPipeline("recursive-custom-resource"), "-w")
		Expect(check).To(gbytes.Say("succeeded"))
	})
})
