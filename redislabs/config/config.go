package config

import (
	"errors"
	"github.com/cloudfoundry-incubator/candiedyaml"
	"os"
	"regexp"
	"strings"
)

type Config struct {
	Cluster       ClusterConfig       `yaml:"cluster"`
	ServiceBroker ServiceBrokerConfig `yaml:"broker"`
	PeerClusters  PeerClustersConfig  `yaml:"peer_clusters"`
}

type PeerClustersConfig struct {
	String   string `yaml:"string"`
	Clusters []ClusterConfig
}

type ClusterConfig struct {
	Auth    AuthConfig `yaml:"auth"`
	Address string     `yaml:"address"`
	Name    string     `yaml:"name"`
}

type ServiceBrokerConfig struct {
	Auth        AuthConfig          `yaml:"auth"`
	Plans       []ServicePlanConfig `yaml:"plans"`
	ServiceID   string              `yaml:"service_id"`
	Port        int                 `yaml:"port"`
	Name        string              `yaml:"name"`
	Description string              `yaml:"description"`
	Metadata    ServiceMetadata     `yaml:"metadata"`
}

type AuthConfig struct {
	Password string `yaml:"password"`
	Username string `yaml:"username"`
}

type ServicePlanConfig struct {
	ID                    string                `yaml:"id"`
	Name                  string                `yaml:"name"`
	Description           string                `yaml:"description"`
	Metadata              ServicePlanMetadata   `yaml:"metadata"`
	ServiceInstanceConfig ServiceInstanceConfig `yaml:"settings"`
}

type ServicePlanMetadata struct {
	Bullets []string `yaml:"bullets"`
}

type ServiceInstanceConfig struct {
	MemoryLimit int64    `yaml:"memory"`
	Replication bool     `yaml:"replication"`
	ShardCount  int64    `yaml:"shard_count"`
	Persistence string   `yaml:"persistence"`
	Snapshot    Snapshot `yaml:"snapshot"`
}

type Snapshot struct {
	Writes int `yaml:"writes"`
	Secs   int `yaml:"secs"`
}

type ServiceMetadata struct {
	DisplayName         string `yaml:"display_name"`
	Image               string `yaml:"image"`
	ProviderDisplayName string `yaml:"provider_display_name"`
}

func parsePeerClustersString(str string) ([]ClusterConfig, error) {
	str_parts := strings.Split(str, ";")
	if len(str_parts) == 0 {
		return []ClusterConfig{}, nil
	}

	re := regexp.MustCompile("(?P<user>.*?):(?P<pass>.*?)@(?P<fqdn>[^/]*)(?P<addr>/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})?")
	clusters := make([]ClusterConfig, len(str_parts))
	for i, part := range str_parts {
		m := re.FindStringSubmatch(part)
		if len(m) == 0 {
			return []ClusterConfig{}, errors.New("Invalid peer cluster string")
		}

		addr := m[3]
		if m[4] != "" {
			addr = m[4][1:]
		}

		clusters[i] = ClusterConfig{
			Auth: AuthConfig{
				Username: m[1],
				Password: m[2]},
			Name:    m[3],
			Address: addr}
	}

	return clusters, nil
}

func LoadFromFile(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}

	var config Config
	if err := candiedyaml.NewDecoder(file).Decode(&config); err != nil {
		return Config{}, err
	}
	if config.PeerClusters.String != "" {
		clusters, err := parsePeerClustersString(config.PeerClusters.String)
		if err != nil {
			return Config{}, err
		}
		config.PeerClusters.Clusters = clusters
	}
	// TODO: add validations here
	return config, nil
}
