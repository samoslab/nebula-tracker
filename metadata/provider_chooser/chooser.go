package provider_chooser

import (
	"fmt"
	"nebula-tracker/db"
	"runtime/debug"
	"sync/atomic"
	"time"

	gosync "github.com/lrita/gosync"
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
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
	// TODO providerMap will be wrong
	// if v, ok := providerMap[nodeId]; ok {
	// 	return v
	// } else {
	return db.ProviderFindOne(nodeId)
	// }
}

var running gosync.Mutex = gosync.NewMutex()

func update() {
	if running.TryLock() {
		defer running.UnLock()
	} else {
		return
	}
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("chooser.update() panic error: %s, detail: %s", er, string(debug.Stack()))
		}
	}()
	all := db.ProviderFindAllAvail()
	providers, providerMap = filter(all)
	// for k, v := range providerMap {
	// 	fmt.Printf("node %s: %s:%d\n", k, v.Host, v.Port)
	// }
	initialized = true
	fmt.Printf("%s found %d available provider.\n", time.Now().UTC().Format("2006-01-02 15:04 UTC"), len(*providers))
}

func filter(all []db.ProviderInfo) (*[]db.ProviderInfo, map[string]*db.ProviderInfo) {
	// slice := make([]db.ProviderInfo, 0, len(all))
	m := make(map[string]*db.ProviderInfo, len(all))
	for i, _ := range all {
		// start := time.Now().UTC()
		// available := false
		// if check(&pi, &available) || check(&pi, &available) || check(&pi, &available) {
		// 	m[pi.NodeId] = &pi
		// 	slice = append(slice, pi)
		// }
		// if !available {
		// 	db.SaveNaRecord(pi.NodeId, start, time.Now().UTC())
		// }
		pi := &all[i]
		m[pi.NodeId] = pi
	}
	return &all, m
}

// func check(pi *db.ProviderInfo, available *bool) bool {
// 	var hostStr string // prefer
// 	if len(pi.Host) > 0 {
// 		hostStr = pi.Host
// 	} else if len(pi.DynamicDomain) > 0 {
// 		hostStr = pi.DynamicDomain
// 	}
// 	providerAddr := fmt.Sprintf("%s:%d", hostStr, pi.Port)
// 	conn, err := grpc.Dial(providerAddr, grpc.WithInsecure())
// 	if err != nil {
// 		return false
// 	}
// 	defer conn.Close()
// 	psc := provider_pb.NewProviderServiceClient(conn)
// 	total, maxFileSize, err := checkAvailable(psc, pi.PublicKey)
// 	if err != nil {
// 		st, ok := status.FromError(err)
// 		if !ok || (st.Code() != codes.DeadlineExceeded && st.Code() != codes.Unavailable) {
// 			fmt.Printf("checkAvailable of provider [%s:%d] failed,  error: %v\n", hostStr, pi.Port, err)
// 		}
// 		return false
// 	}
// 	*available = true
// 	if total > giga && maxFileSize > giga {
// 		return true
// 	} else {
// 		fmt.Printf("checkAvailable of provider [%s:%d] reply total: %d, maxFileSize: %d\n", hostStr, pi.Port, total, maxFileSize)
// 		return false
// 	}
// }

// var giga uint64 = 1024 * 1024 * 1024

// func checkAvailable(client provider_pb.ProviderServiceClient, publicKeyBytes []byte) (total uint64, maxFileSize uint64, err error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
// 	defer cancel()
// 	req := &provider_pb.CheckAvailableReq{Timestamp: uint64(time.Now().Unix())}
// 	req.GenAuth(publicKeyBytes)
// 	var resp *provider_pb.CheckAvailableResp
// 	resp, err = client.CheckAvailable(ctx, req)
// 	if err != nil {
// 		return
// 	}
// 	return resp.Total, resp.MaxFileSize, nil
// }
