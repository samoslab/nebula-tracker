package config

import (
	"fmt"

	"github.com/koding/multiconfig"
)

const config_filename = "config.toml"

var initTrackerConfig = false
var trackerConfig *TrackerConfig

type TrackerConfig struct {
	Db       Db
	Server   Server
	Smtps    Smtps
	TestMode bool `default:"false"`
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
}

type Server struct {
	ListenIp   string `default:"127.0.0.1"`
	ListenPort int    `default:"6677"`
}

type Smtps struct {
	Host     string `default:"smtp.163.com"`
	Port     int    `default:"465"`
	Username string `default:"silveradmin@163.com"`
	Password string `default:"unknown"`
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
