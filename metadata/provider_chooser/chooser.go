package provider_chooser

import (
	"fmt"
	"nebula-tracker/db"
	"time"

	"github.com/robfig/cron"
)

var cronRunner *cron.Cron

func StartAutoUpdate() {
	cronRunner = cron.New()
	cronRunner.AddFunc("15 */3 * * * *", update)
	cronRunner.Start()
}

func StopAutoUpdate() {
	cronRunner.Stop()
}

var providers *[]db.ProviderInfo
var providerMap map[string]*db.ProviderInfo

func Count() int {
	return len(*providers)
}

func Choose(num uint32) []db.ProviderInfo {
	// TODO
	return (*providers)[0:num]
}

func Get(nodeId string) *db.ProviderInfo {
	if v, ok := providerMap[nodeId]; ok {
		return v
	} else {
		return db.ProviderFindOne(nodeId)
	}
}

func update() {
	all := db.ProviderFindAll()
	providers, providerMap = filter(all)
	fmt.Printf("%s found %d available provider.\n", time.Now().UTC().Format("2006-01-02 15:04 UTC"), len(*providers))
}

func filter(all []db.ProviderInfo) (*[]db.ProviderInfo, map[string]*db.ProviderInfo) {
	m := make(map[string]*db.ProviderInfo, len(all))
	for _, pi := range all {
		m[pi.NodeId] = &pi
	}
	return &all, m
}
