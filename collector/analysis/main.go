package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"nebula-tracker/collector/config"
	"nebula-tracker/collector/db"
	"os"
	"path/filepath"
	"time"

	pb "nebula-tracker/api/collector/pb"

	"github.com/gogo/protobuf/proto"
	"github.com/koding/multiconfig"
	util_aes "github.com/samoslab/nebula/util/aes"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var encryptKey []byte
var apiToken []byte
var apiHostAndPort []string

func main() {
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	conf := GetConfig(path + string(os.PathSeparator) + "config.toml")

	ti := conf.TrackerInterface
	encryptKey, err = hex.DecodeString(ti.EncryptKeyHex)
	if err != nil {
		log.Fatalf("decode encrypt key Error： %s", err)
	}
	if len(encryptKey) != 16 && len(encryptKey) != 24 && len(encryptKey) != 32 {
		log.Fatalf("encrypt key length Error： %d", len(encryptKey))
	}
	apiToken = []byte(ti.ApiToken)
	if len(apiToken) == 0 {
		log.Fatalf("ApiToken is required")
	}
	apiHostAndPort = ti.ApiHostAndPort
	if len(apiHostAndPort) == 0 {
		log.Fatalf("ApiHostAndPort is required")
	}

	dbo := db.OpenDb(&conf.Db)
	defer dbo.Close()
	analysis(int64(conf.BatchSecs))
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
	BatchSecs        int `default:"1800"`
}

func getNextAnalysisStart() (int64, error) {
	conn, err := grpc.Dial(apiHostAndPort[0], grpc.WithInsecure())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	client := pb.NewForCollectorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.NextAnalysisStartReq{Timestamp: uint64(time.Now().Unix())}
	req.GenAuth(apiToken)
	resp, err := client.NextAnalysisStart(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.Start, nil
}

func hourlyUpdate(data []byte) error {
	conn, err := grpc.Dial(apiHostAndPort[0], grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewForCollectorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req := &pb.HourlyUpdateReq{Timestamp: uint64(time.Now().Unix()), Data: data}
	req.GenAuth(apiToken)
	_, err = client.HourlyUpdate(ctx, req)
	return err
}

const KEY_LAST_START string = "collector-next-analysis-start"

func analysis(batchSecs int64) {
	var start int64
	var err error
	for i := 0; i < 5; i++ {
		start, err = getNextAnalysisStart()
		if err == nil {
			break
		} else {
			log.Warnf("getNextAnalysisStart, times: %d, error: %v", err)
		}
	}
	if err != nil {
		panic(err)
	}
	if start == 0 {
		// start = 1533891600 //2018/8/10 9:00:00 UTC
		panic("zero start")
	}
	current := time.Now().Unix() - 600
	for nextStart := start + batchSecs; nextStart < current; nextStart += batchSecs {
		hs := db.HouryNaSummarize(start, nextStart)
		fmt.Printf("Start: %d, NextStart: %d, Provider Count: %d, Client Count: %d\n", hs.Start, hs.NextStart, len(hs.ProviderItem), len(hs.ClientItem))
		data, err := proto.Marshal(hs)
		if err != nil {
			panic(err)
		}
		en, err := util_aes.Encrypt(data, encryptKey)
		if err != nil {
			panic(err)
		}
		for i := 0; i < 5; i++ {
			err = hourlyUpdate(en)
			if err == nil {
				start = nextStart
				break
			} else {
				log.Warnf("hourlyUpdate, times: %d, error: %v", i, err)
			}
		}
		if err != nil {
			panic(err)
		}
	}
}
