package redislabs_test

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/Altoros/cf-redislabs-broker/redislabs"
	brokerconfig "github.com/Altoros/cf-redislabs-broker/redislabs/config"
	"github.com/Altoros/cf-redislabs-broker/redislabs/instance_creators"
	"github.com/Altoros/cf-redislabs-broker/redislabs/persisters"
	"github.com/pivotal-cf/brokerapi"
	"github.com/pivotal-golang/lager"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Broker", func() {
	var (
		broker          redislabs.ServiceBroker
		config          brokerconfig.Config
		instanceCreator redislabs.ServiceInstanceCreator
		instanceBinder  redislabs.ServiceInstanceBinder
		persister       persisters.StatePersister
		logger          = lager.NewLogger("test") // does not actually log anything
	)

	JustBeforeEach(func() {
		broker = redislabs.ServiceBroker{
			Config:          config,
			InstanceCreator: instanceCreator,
			InstanceBinder:  instanceBinder,
			StatePersister:  persister,
			Logger:          logger,
		}
	})

	Describe("Looking for plans", func() {
		Context("Given a config with one default plan", func() {
			BeforeEach(func() {
				config = brokerconfig.Config{
					ServiceBroker: brokerconfig.ServiceBrokerConfig{
						Plans: []brokerconfig.ServicePlanConfig{
							{
								ID:          "",
								Name:        "",
								Description: "",
							},
						},
					},
				}
			})
			It("Offers a service with at least one plan to use", func() {
				Expect(len(broker.Services())).To(Equal(1))
				Expect(len(broker.Services()[0].Plans)).ToNot(Equal(0))
			})
		})
	})

	Describe("Provisioning an instance", func() {
		var (
			serviceID                           = "test-service-id"
			planID                              = "test-plan-id"
			requestedServiceID, requestedPlanID string
			details                             brokerapi.ProvisionDetails
		)
		Context("Given a config with a default plan", func() {
			BeforeEach(func() {
				config = brokerconfig.Config{
					ServiceBroker: brokerconfig.ServiceBrokerConfig{
						ServiceID: serviceID,
						Plans: []brokerconfig.ServicePlanConfig{
							{
								ID:          planID,
								Name:        "test",
								Description: "Lorem ipsum dolor sit amet",
							},
						},
					},
				}
			})
			JustBeforeEach(func() {
				details = brokerapi.ProvisionDetails{
					ID:               requestedServiceID,
					PlanID:           requestedPlanID,
					OrganizationGUID: "",
					SpaceGUID:        "",
				}
			})
			Context("And a wrong service ID", func() {
				BeforeEach(func() {
					requestedServiceID = "unknown"
				})
				It("Rejects to create an instance", func() {
					err := broker.Provision("some-id", details)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})

			Context("And a wrong plan ID", func() {
				BeforeEach(func() {
					requestedServiceID = serviceID
					requestedPlanID = "unknown"
				})
				It("Rejects to create an instance", func() {
					err := broker.Provision("some-id", details)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(redislabs.ErrPlanDoesNotExist))
				})
			})

			Context("And no instance creators", func() {
				BeforeEach(func() {
					requestedServiceID = serviceID
					requestedPlanID = planID
				})
				It("Rejects to create an instance", func() {
					err := broker.Provision("some-id", details)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(redislabs.ErrInstanceCreatorNotFound))
				})
			})

			Context("And no persisters", func() {
				BeforeEach(func() {
					requestedServiceID = serviceID
					requestedPlanID = planID
					instanceCreator = instancecreators.NewDefault(config, logger)
				})
				It("Rejects to create an instance", func() {
					err := broker.Provision("some-id", details)
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(redislabs.ErrPersisterNotFound))
				})
			})

			Context("And given proper settings", func() {
				var (
					tmpStateDir string
				)

				BeforeEach(func() {
					requestedServiceID = serviceID
					requestedPlanID = planID
					var err error
					tmpStateDir, err = ioutil.TempDir("", "redislabs-state-test")
					Expect(err).NotTo(HaveOccurred())
					persister = persisters.NewLocalPersister(path.Join(tmpStateDir, "state.json"))
				})

				AfterEach(func() {
					os.RemoveAll(tmpStateDir)
				})

				It("Creates an instance of the configured default plan", func() {
					err := broker.Provision("some-id", details)
					Expect(err).ToNot(HaveOccurred())
				})
				It("Rejects to provision the same instance again", func() {
					broker.Provision("some-id", details)
					err := broker.Provision("some-id", details)
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})

})
