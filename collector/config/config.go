package config

import (
	"fmt"

	"github.com/koding/multiconfig"
)

const config_filename = "config.toml"

var initCollectorConfig = false
var collectorConfig *CollectorConfig

type CollectorConfig struct {
	Db               Db
	Server           Server
	TrackerInterface TrackerInterface
	NsqAddrs         []string
	TestMode         bool `default:"false"`
}

type Db struct {
	Host            string `default:"localhost"`
	Port            int    `default:"26257"`
	User            string `default:"root"`
	Password        string `default:""`
	Name            string `default:"collector"`
	ApplicationName string `default:"cockroach"`
	SslMode         string `default:"disable"`
	MaxOpenConns    int    `default:"500"`
	MaxIdleConns    int    `default:"50"`
	SslCert         string
	SslRootCert     string
	SslKey          string
}

type Server struct {
	ListenIp   string `default:"127.0.0.1"`
	ListenPort int    `default:"6688"`
}

type TrackerInterface struct {
	ContextPath   string `default:"http://localhost:6655/api"`
	ApiToken      string `default:"test"`
	EncryptKeyHex string `default:"4fcf16120e28dec237da6ecdcb7ec3be"`
	Debug         bool   `default:"false"`
}

func GetCollectorConfig() *CollectorConfig {
	if initCollectorConfig {
		return collectorConfig
	}
	m := multiconfig.NewWithPath(config_filename) // supports TOML, JSON and YAML
	collectorConfig = new(CollectorConfig)
	err := m.Load(collectorConfig) // Check for error
	if err != nil {
		fmt.Printf("GetCollectorConfig Error: %s\n", err)
	}
	m.MustLoad(collectorConfig) // Panic's if there is any error
	//	fmt.Printf("%+v\n", config)
	initCollectorConfig = true
	return collectorConfig
}
