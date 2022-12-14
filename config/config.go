package config

import (
	"fmt"

	"github.com/koding/multiconfig"
)

const config_filename = "config.toml"

var initTrackerConfig = false
var trackerConfig *TrackerConfig

type TrackerConfig struct {
	Db                   Db
	Server               Server
	Smtps                Smtps
	AddressChecksumToken string `default:"test-checksum-token"` // for testing convenience, must be specified other string in config.toml
	TestMode             bool   `default:"false"`
}

type Db struct {
	Host            string `default:"localhost"`
	Port            int    `default:"26257"`
	User            string `default:"root"`
	Password        string `default:""`
	Name            string `default:"tracker"`
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
	ListenPort int    `default:"6677"`
}

type Smtps struct {
	Host     string `default:"smtp.163.com"`
	Port     int    `default:"465"`
	Username string `default:"silveradmin@163.com"`
	Password string `default:"adminsilver"`
}

func GetTrackerConfig() *TrackerConfig {
	if initTrackerConfig {
		return trackerConfig
	}
	m := multiconfig.NewWithPath(config_filename) // supports TOML, JSON and YAML
	trackerConfig = new(TrackerConfig)
	err := m.Load(trackerConfig) // Check for error
	if err != nil {
		fmt.Printf("GetTrackerConfig Error: %s\n", err)
	}
	m.MustLoad(trackerConfig) // Panic's if there is any error
	//	fmt.Printf("%+v\n", config)
	initTrackerConfig = true
	return trackerConfig
}

var initInterfaceConfig = false
var interfaceConfig *InterfaceConfig

type InterfaceConfig struct {
	Db            Db
	ListenIp      string `default:"127.0.0.1"`
	ListenPort    int    `default:"6655"`
	AuthValidSec  int    `default:"15"`
	AuthToken     string `default:"test"`
	EncryptKeyHex string `default:"4fcf16120e28dec237da6ecdcb7ec3be"`
	TestMode      bool   `default:"false"`
}

func GetInterfaceConfig() *InterfaceConfig {
	if initInterfaceConfig {
		return interfaceConfig
	}
	m := multiconfig.NewWithPath(config_filename) // supports TOML, JSON and YAML
	interfaceConfig = new(InterfaceConfig)
	err := m.Load(interfaceConfig) // Check for error
	if err != nil {
		fmt.Printf("GetInterfaceConfig Error: %s\n", err)
	}
	m.MustLoad(interfaceConfig) // Panic's if there is any error
	//	fmt.Printf("%+v\n", config)
	initInterfaceConfig = true
	return interfaceConfig
}

type ApiForTellerConfig struct {
	Db                   Db
	ListenIp             string `default:"127.0.0.1"`
	ListenPort           int    `default:"6699"`
	AuthToken            string
	AuthValidSec         int    `default:"15"`
	AddressChecksumToken string `default:"test-checksum-token"` // for testing convenience, must be specified other string in config.toml
	Debug                bool   `default:"false"`
}

var initApiForTellerConfig = false
var apiForTellerConfig *ApiForTellerConfig

func GetApiForTellerConfig() *ApiForTellerConfig {
	if initApiForTellerConfig {
		return apiForTellerConfig
	}
	m := multiconfig.NewWithPath(config_filename) // supports TOML, JSON and YAML
	apiForTellerConfig = new(ApiForTellerConfig)
	err := m.Load(apiForTellerConfig) // Check for error
	if err != nil {
		fmt.Printf("GetInterfaceConfig Error: %s\n", err)
	}
	m.MustLoad(apiForTellerConfig) // Panic's if there is any error
	//	fmt.Printf("%+v\n", config)
	initApiForTellerConfig = true
	return apiForTellerConfig
}
