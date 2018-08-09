package main

import (
	"nebula-tracker/collector/config"
	"nebula-tracker/collector/db"
	"os"
	"path/filepath"

	"github.com/koding/multiconfig"
)

func main() {
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	conf := GetConfig(path + string(os.PathSeparator) + "config.toml")
	dbo := db.OpenDb(&conf.Db)
	defer dbo.Close()
	// db.DailyNaSummarize(int64(conf.NaThreshold), int64(conf.Offset))
}

func GetConfig(path string) *Config {
	m := multiconfig.NewWithPath(path) // supports TOML, JSON and YAML
	conf := new(Config)
	err := m.Load(conf) // Check for error
	if err != nil {
		panic(err)
	}
	m.MustLoad(conf) // Panic's if there is any error
	//	fmt.Printf("%+v\n", config)
	return conf
}

type Config struct {
	Db               config.Db
	TrackerInterface config.TrackerInterface
}
