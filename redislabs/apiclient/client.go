package apiclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/RedisLabs/cf-redislabs-broker/redislabs/cluster"
	"github.com/RedisLabs/cf-redislabs-broker/redislabs/config"
	"github.com/RedisLabs/cf-redislabs-broker/redislabs/httpclient"
	"github.com/pivotal-golang/lager"
)

type apiClient struct {
	logger       lager.Logger
	httpClient   httpclient.HTTPClient
	crdbClusters []crdbClusterInfo
}

type Client interface {
	CreateDatabase(map[string]interface{}) (chan cluster.InstanceCredentials, error)
	UpdateDatabase(string, map[string]interface{}) error
	DeleteDatabase(string) error
	GetDatabase(string) (cluster.InstanceCredentials, error)
}

type errorResponse struct {
	ErrorMessage string `json:"description"`
	ErrorCode    string `json:"error_code"`
}

type endpointResponse struct {
	DNSName  string   `json:"dns_name"`
	Port     int      `json:"port"`
	AddrList []string `json:"addr"`
}

type statusResponse struct {
	UID       int                `json:"uid"`
	CRDBGUID  string             `json:"crdt_guid"` // Note: crdt_guid is not a typo
	Password  string             `json:"authentication_redis_pass"`
	Endpoints []endpointResponse `json:"endpoints"`
	Status    string             `json:"status"`
}

var (
	DatabasePollingInterval = 500 // milliseconds

	errDbIsNotActive = errors.New("db is not active")
)

func New(conf config.Config, logger lager.Logger) Client {
	httpClient := httpclient.New(
		conf.Cluster.Auth.Username,
		conf.Cluster.Auth.Password,
		conf.Cluster.Address,
		logger,
	)

	return &apiClient{
		logger:       logger,
		httpClient:   httpClient,
		crdbClusters: makeCRDBClusters(conf),
	}
}

func (c *apiClient) CreateDatabase(settings map[string]interface{}) (chan cluster.InstanceCredentials, error) {
	if settings["type"] == "crdb" {
		delete(settings, "type")
		return c.CreateCRDB(settings)
	}
	bytes, err := json.Marshal(settings)
	if err != nil {
		return nil, err
	}

	c.logger.Info("Sending a database creation request", lager.Data{
		"settings": settings,
	})
	res, err := c.httpClient.Post("/v1/bdbs", httpclient.HTTPPayload(bytes))
	if err != nil {
		c.logger.Error("Failed to perform a database creation request", err)
		return nil, err
	}

	var dbUid string

	if res.StatusCode != 200 {
		payload, err := c.parseErrorResponse(res)
		if err != nil {
			return nil, err
		}
		err = fmt.Errorf(payload.ErrorMessage)
		c.logger.Error("Failed to create a database", err)
		return nil, err
	} else {
		payload, err := c.parseStatusResponse(res)
		if err != nil {
			return nil, err
		}

		dbUid = strconv.Itoa(payload.UID)
	}

	c.logger.Info("Database creation has been scheduled")

	ch := make(chan cluster.InstanceCredentials)
	go func() {
		for {
			time.Sleep(time.Duration(DatabasePollingInterval) * time.Millisecond)

			instanceCredentials, err := c.GetDatabase(dbUid)
			if err != nil {
				if err == errDbIsNotActive {
					c.logger.Info("Database is not active yet")
				} else {
					c.logger.Error("Failed to make a polling request", err)
				}
			} else {
				ch <- instanceCredentials
				break
			}
		}
	}()
	return ch, nil
}

func (c *apiClient) UpdateDatabase(UID string, params map[string]interface{}) error {
	bytes, err := json.Marshal(params)
	if err != nil {
		c.logger.Error("Failed to serialize update parameters", err)
	}

	c.logger.Info("Sending a database update request", lager.Data{
		"UID":        UID,
		"Parameters": params,
	})
	res, err := c.httpClient.Put(fmt.Sprintf("/v1/bdbs/%s", UID), httpclient.HTTPPayload(bytes))
	if err != nil {
		c.logger.Error("Failed to perform an update request", err, lager.Data{
			"UID": UID,
		})
		return err
	}

	if res.StatusCode != 200 {
		payload, err := c.parseErrorResponse(res)
		if err != nil {
			return err
		}
		err = fmt.Errorf(payload.ErrorMessage)
		c.logger.Error("Failed to update the database", err, lager.Data{
			"UID": UID,
		})
		return err
	}

	c.logger.Info("The database update has been scheduled", lager.Data{
		"UID": UID,
	})
	return nil
}

func (c *apiClient) GetDatabase(UID string) (cluster.InstanceCredentials, error) {
	res, err := c.httpClient.Get(fmt.Sprintf("/v1/bdbs/%s", UID), httpclient.HTTPParams{})
	if err != nil {
		return cluster.InstanceCredentials{}, fmt.Errorf("failed to query API for db '%s' details: %s", UID, err)
	}

	payload, err := c.parseStatusResponse(res)
	if err != nil {
		return cluster.InstanceCredentials{}, fmt.Errorf("failed to parse DB '%s' response: %s", UID, err)
	}

	if payload.Status != "active" {
		fmt.Println("db statsus=", payload.Status)
		return cluster.InstanceCredentials{}, errDbIsNotActive
	}

	if len(payload.Endpoints) < 1 {
		return cluster.InstanceCredentials{}, fmt.Errorf("No endpoints created")
	}

	return cluster.InstanceCredentials{
		UID:      strconv.Itoa(payload.UID),
		Host:     payload.Endpoints[0].DNSName,
		Port:     payload.Endpoints[0].Port,
		IPList:   payload.Endpoints[0].AddrList,
		Password: payload.Password,
	}, nil
}

func (c *apiClient) DeleteDatabase(UID string) error {
	if isCRDBUID(UID) {
		return c.DeleteCRDB(UID)
	}

	res, err := c.httpClient.Delete(fmt.Sprintf("/v1/bdbs/%s", UID))
	if err != nil {
		c.logger.Error("Failed to perform the database removal request", err, lager.Data{
			"UID": UID,
		})
		return err
	}

	if res.StatusCode != 200 {
		payload, err := c.parseErrorResponse(res)
		if err != nil {
			return err
		}
		err = fmt.Errorf(payload.ErrorMessage)
		c.logger.Error("Failed to delete the database", err)
		return err
	}

	c.logger.Info("The database removal has been scheduled", lager.Data{
		"UID": UID,
	})
	return nil
}

func (c *apiClient) parseErrorResponse(res *http.Response) (errorResponse, error) {
	payload := errorResponse{}
	bytes, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err == nil {
		err = json.Unmarshal(bytes, &payload)
	}
	if err != nil {
		c.logger.Error("Failed to parse the error response payload", err, lager.Data{
			"response": string(bytes),
		})
		err = fmt.Errorf("an unknown server error occurred")
	}
	return payload, err
}

func (c *apiClient) parseStatusResponse(res *http.Response) (statusResponse, error) {
	payload := statusResponse{}
	bytes, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err == nil {
		err = json.Unmarshal(bytes, &payload)
	}
	if err != nil {
		c.logger.Error("Failed to parse the status response payload", err)
	}
	return payload, err
}
