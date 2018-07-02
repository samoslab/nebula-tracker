package provider_chooser

import (
	"context"
	"fmt"
	"nebula-tracker/db"
	"sync/atomic"
	"time"

	gosync "github.com/lrita/gosync"
	"github.com/robfig/cron"
	provider_pb "github.com/samoslab/nebula/provider/pb"
	"google.golang.org/grpc"
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
var initialized = false
var currentProviderIdx uint64 = 0

func incrementProviderIdx(offset uint64) {
	atomic.AddUint64(&currentProviderIdx, offset)
}
func Count() int {
	if !initialized {
		update()
	}
	return len(*providers)
}

func Choose(num int) []db.ProviderInfo {
	if !initialized {
		update()
	}
	pros := *providers
	l := len(pros)
	if l < num {
		panic("provider is not enough")
	}
	idx := int(currentProviderIdx % uint64(l))
	incrementProviderIdx(uint64(num))
	if idx+num <= l {
		return pros[idx : idx+num]
	} else {
		k := l - idx
		res := make([]db.ProviderInfo, num)
		copy(res[0:k], pros[idx:l])
		copy(res[k:num], pros[0:num-k])
		return res
	}
	// return (*providers)[0:num]
}

func Get(nodeId string) *db.ProviderInfo {
	if v, ok := providerMap[nodeId]; ok {
		return v
	} else {
		return db.ProviderFindOne(nodeId)
	}
}

var running gosync.Mutex = gosync.NewMutex()

func update() {
	if running.TryLock() {
		defer running.UnLock()
	} else {
		return
	}
	all := db.ProviderFindAll()
	providers, providerMap = filter(all)
	initialized = true
	fmt.Printf("%s found %d available provider.\n", time.Now().UTC().Format("2006-01-02 15:04 UTC"), len(*providers))
}

func filter(all []db.ProviderInfo) (*[]db.ProviderInfo, map[string]*db.ProviderInfo) {
	slice := make([]db.ProviderInfo, 0, len(all))
	m := make(map[string]*db.ProviderInfo, len(all))
	for _, pi := range all {
		if check(&pi) || check(&pi) || check(&pi) {
			m[pi.NodeId] = &pi
			slice = append(slice, pi)
		}
	}
	return &slice, m
}

func check(pi *db.ProviderInfo) bool {
	var hostStr string // prefer
	if len(pi.Host) > 0 {
		hostStr = pi.Host
	} else if len(pi.DynamicDomain) > 0 {
		hostStr = pi.DynamicDomain
	}
	providerAddr := fmt.Sprintf("%s:%d", hostStr, pi.Port)
	conn, err := grpc.Dial(providerAddr, grpc.WithInsecure())
	if err != nil {
		return false
	}
	defer conn.Close()
	psc := provider_pb.NewProviderServiceClient(conn)
	return pingProvider(psc) == nil
}

func pingProvider(client provider_pb.ProviderServiceClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.Ping(ctx, &provider_pb.PingReq{})
	return err
}
