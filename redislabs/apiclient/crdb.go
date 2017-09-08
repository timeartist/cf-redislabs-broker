package apiclient

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/RedisLabs/cf-redislabs-broker/redislabs/cluster"
	"github.com/RedisLabs/cf-redislabs-broker/redislabs/config"
	"github.com/RedisLabs/cf-redislabs-broker/redislabs/httpclient"
	"github.com/pivotal-golang/lager"
)

type crdbErrorResponse struct {
	ClusterName string `json:"cluster_name"`
	ErrorCode   string `json:"error_code"`
	Description string `json:"description"`
}

type crdbTaskStatus struct {
	ID       string              `json:"id"`
	CRDBGUID string              `json:"crdb_guid"`
	Status   string              `json:"status"`
	Errors   []crdbErrorResponse `json:"errors"`
}

type crdbSettings struct {
	GUID            string                 `json:"guid,omitempty"`
	Name            string                 `json:"name"`
	DefaultDBConfig map[string]interface{} `json:"default_db_config,omitifempty"`
	Instances       []crdbInstance         `json:"instances"`
}

type crdbInstance struct {
	ID          int                    `json:"id,omitifempty"`
	Compression int                    `json:"compression,omitifempty"`
	Cluster     crdbClusterInfo        `json:"cluster"`
	DBConfig    map[string]interface{} `json:"db_config,omitifempty"`
}

type crdbClusterInfo struct {
	Name        string                 `json:"name"`
	URL         string                 `json:"url"`
	Credentials crdbClusterCredentials `json:"credentials"`
}

type crdbClusterCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func isCRDBUID(UID string) bool {
	return strings.Contains(UID, "-")
}

func makeCRDBClusters(conf config.Config) []crdbClusterInfo {
	if len(conf.PeerClusters.Clusters) == 0 {
		return []crdbClusterInfo{}
	}

	result := make([]crdbClusterInfo, len(conf.PeerClusters.Clusters))
	for i, cluster := range conf.PeerClusters.Clusters {
		result[i] = crdbClusterInfo{
			Name: cluster.Name,
			URL:  fmt.Sprintf("http://%s:8080", cluster.Address),
			Credentials: crdbClusterCredentials{
				Username: cluster.Auth.Username,
				Password: cluster.Auth.Password,
			},
		}
	}

	return result
}

func (c *apiClient) CreateCRDB(settings map[string]interface{}) (chan cluster.InstanceCredentials, error) {

	crdb := crdbSettings{
		Name:            settings["name"].(string),
		DefaultDBConfig: settings,
		Instances:       make([]crdbInstance, len(c.crdbClusters)),
	}

	for i, cluster := range c.crdbClusters {
		crdb.Instances[i] = crdbInstance{
			Cluster: cluster,
		}
	}

	bytes, err := json.Marshal(crdb)
	if err != nil {
		return nil, err
	}

	c.logger.Info("Sending a CRDB creation request", lager.Data{
		"crdb": crdb,
	})

	res, err := c.httpClient.Post("/v1/crdbs", httpclient.HTTPPayload(bytes))
	if err != nil {
		c.logger.Error("Failed to perform a CRDB creation request", err)
		return nil, err
	}

	var crdbGUID string
	var taskID string

	if res.StatusCode != 200 {
		payload, err := c.parseCRDBErrorResponse(res)
		if err != nil {
			return nil, err
		}
		err = fmt.Errorf(payload.Description)
		c.logger.Error("Failed to create a CRDB", err)
		return nil, err
	} else {
		payload, err := c.parseTaskStatusResponse(res)
		if err != nil {
			return nil, err
		}

		crdbGUID = payload.CRDBGUID
		taskID = payload.ID
	}

	c.logger.Info("CRDB creation has been scheduled, ", lager.Data{
		"task_id":   taskID,
		"crdb_guid": crdbGUID})

	ch := make(chan cluster.InstanceCredentials)
	go func() {
		for {
			time.Sleep(time.Duration(DatabasePollingInterval) * time.Millisecond)

			status, err := c.GetCRDBTaskStatus(taskID)
			if err != nil {
				c.logger.Error("Failed to make a polling request", err)
			} else if status.Status == "finished" {
				instanceCredentials, _ := c.GetCRDBSettings(crdbGUID)
				ch <- instanceCredentials
				break
			}
		}
	}()
	return ch, nil

}

func (c *apiClient) parseCRDBErrorResponse(res *http.Response) (crdbErrorResponse, error) {
	payload := crdbErrorResponse{}
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

func (c *apiClient) parseTaskStatusResponse(res *http.Response) (crdbTaskStatus, error) {
	payload := crdbTaskStatus{}
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

func (c *apiClient) GetCRDBTaskStatus(ID string) (crdbTaskStatus, error) {
	res, err := c.httpClient.Get(fmt.Sprintf("/v1/crdb_tasks/%s", ID), httpclient.HTTPParams{})
	if err != nil {
		return crdbTaskStatus{}, fmt.Errorf("failed to query API for task '%s' details: %s", ID, err)
	}

	payload, err := c.parseTaskStatusResponse(res)
	if err != nil {
		return crdbTaskStatus{}, fmt.Errorf("failed to parse task '%s' response: %s", ID, err)
	}

	return payload, nil
}

func (c *apiClient) GetCRDBSettings(GUID string) (cluster.InstanceCredentials, error) {
	// The CRDB Management API currently does not return database settings, nor provides
	// a way to map a CRDB GUID to local database IDs.  As a workaround we fetch all bdbs
	// and look for the first one to match the CRDB GUID.
	res, err := c.httpClient.Get("/v1/bdbs", httpclient.HTTPParams{})
	if err != nil {
		return cluster.InstanceCredentials{}, fmt.Errorf("failed to query API for bdbs, details: %s", err)
	}

	payload := []statusResponse{}
	bytes, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err == nil {
		err = json.Unmarshal(bytes, &payload)
	}
	if err != nil {
		c.logger.Error("Failed to parse bdbs response", err)
		return cluster.InstanceCredentials{}, err
	}

	// Find database
	for _, db := range payload {
		c.logger.Debug("Received local DB instance", lager.Data{"db": db})
		if db.CRDBGUID == GUID {
			if db.Status != "active" {
				return cluster.InstanceCredentials{}, errDbIsNotActive
			}
			if len(db.Endpoints) < 1 {
				return cluster.InstanceCredentials{}, fmt.Errorf("No endpoints created")
			}
			return cluster.InstanceCredentials{
				UID:      GUID,
				Host:     db.Endpoints[0].DNSName,
				Port:     db.Endpoints[0].Port,
				IPList:   db.Endpoints[0].AddrList,
				Password: db.Password,
			}, nil
		}
	}

	return cluster.InstanceCredentials{}, fmt.Errorf("DB not found")
}

func (c *apiClient) DeleteCRDB(GUID string) error {
	res, err := c.httpClient.Delete(fmt.Sprintf("/v1/crdbs/%s", GUID))
	if err != nil {
		c.logger.Error("Failed to perform the CRDB removal request", err, lager.Data{
			"GUID": GUID,
		})
		return err
	}

	if res.StatusCode != 200 {
		payload, err := c.parseCRDBErrorResponse(res)
		if err != nil {
			return err
		}
		err = fmt.Errorf(payload.Description)
		c.logger.Error("Failed to delete the CRDB", err)
		return err
	}

	payload, err := c.parseTaskStatusResponse(res)
	if err != nil {
		return err
	}

	taskID := payload.ID

	c.logger.Info("The CRDB removal has been scheduled", lager.Data{
		"GUID":   GUID,
		"TaskID": taskID,
	})

	// We don't wait for it to be removed
	return nil
}
