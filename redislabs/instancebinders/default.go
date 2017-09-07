package instancebinders

import (
	"github.com/pivotal-cf/brokerapi"
	"github.com/pivotal-golang/lager"

	"github.com/RedisLabs/cf-redislabs-broker/redislabs/apiclient"
	"github.com/RedisLabs/cf-redislabs-broker/redislabs/config"
	"github.com/RedisLabs/cf-redislabs-broker/redislabs/persisters"
)

type defaultBinder struct {
	logger    lager.Logger
	apiClient apiclient.Client
}

func NewDefault(conf config.Config, logger lager.Logger) *defaultBinder {
	return &defaultBinder{
		logger:    logger,
		apiClient: apiclient.New(conf, logger),
	}
}

func (d *defaultBinder) Unbind(instanceID string, bindingID string, persister persisters.StatePersister) error {
	return nil
}

func (d *defaultBinder) InstanceExists(instanceID string, persister persisters.StatePersister) (bool, error) {
	return false, nil
}

func (d *defaultBinder) Bind(instanceID string, bindingID string, persister persisters.StatePersister) (interface{}, error) {
	state, err := persister.Load()
	if err != nil {
		d.logger.Error("Failed to load the broker state", err)
		return nil, err
	}
	for _, instance := range state.AvailableInstances {
		if instance.ID == instanceID {
			creds := instance.Credentials
			d.logger.Info("Returning the service credentials", lager.Data{"credentials": creds})

			return map[string]interface{}{
				"host":     d.getHost(creds.UID, creds.Host),
				"port":     creds.Port,
				"ip_list":  creds.IPList,
				"password": creds.Password,
			}, nil
		}
	}
	return nil, brokerapi.ErrInstanceDoesNotExist
}

func (d *defaultBinder) getHost(UID string, host string) string {
	// if state file contains host just return it
	if len(host) != 0 {
		return host
	}

	// if service instance was created before this update state file
	// does not contain host. Fetch it here from RLEC
	instanceCredentials, err := d.apiClient.GetDatabase(UID)
	if err != nil {
		d.logger.Error("Failed to get instance details from API", err)
		return ""
	}

	return instanceCredentials.Host
}
