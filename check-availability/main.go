package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "nebula-tracker/api/check-availability/pb"

	"github.com/koding/multiconfig"
	"github.com/robfig/cron"
	provider_pb "github.com/samoslab/nebula/provider/pb"
	util_aes "github.com/samoslab/nebula/util/aes"
	"golang.org/x/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	conf = GetConfig()

	authToken = []byte(conf.AuthToken)
	var err error
	encryptKey, err = hex.DecodeString(conf.EncryptKeyHex)
	if err != nil {
		panic(err)
	}
	var cronRunner = cron.New()
	cronRunner.AddFunc(conf.CronExp, check)
	cronRunner.Start()
	defer cronRunner.Stop()
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
}

var authToken []byte
var encryptKey []byte
var conf *Config

func check() {
	var ps []*pb.Provider
	var err error
	for i := 0; i < 5; i++ {
		ps, err = getProvider()
		if err != nil {
			fmt.Printf("getProvider error: %v\n", err)
		}
	}
	if ps == nil || len(ps) == 0 {
		return
	}
	pss := make([]*pb.ProviderStatus, 0, len(ps))
	for _, pi := range ps {
		var hostStr string // prefer
		if len(pi.Host) > 0 {
			hostStr = pi.Host
		} else if len(pi.DynamicDomain) > 0 {
			hostStr = pi.DynamicDomain
		}
		start := time.Now().UnixNano()
		total, maxFileSize, err := checkProvider(hostStr, pi.Port, pi.PublicKey)
		if err == nil {
			pss = append(pss, &pb.ProviderStatus{NodeId: pi.NodeId,
				CheckTime:       uint64(start),
				LatencyNs:       uint64(time.Now().UnixNano() - start),
				TotalFreeVolume: total,
				AvailFileSize:   maxFileSize})
		} else {
			st, ok := status.FromError(err)
			if ok {
				if st.Code() == codes.Unavailable {
					fmt.Printf("NodeId: %s, %s:%d unavailable\n", pi.NodeId, hostStr, pi.Port)
				} else if st.Code() == codes.DeadlineExceeded {
					fmt.Printf("NodeId: %s, %s:%d DeadlineExceeded\n", pi.NodeId, hostStr, pi.Port)
				} else {
					fmt.Printf("NodeId: %s, %s:%d Code: %d, Message: %s\n", pi.NodeId, hostStr, pi.Port, st.Code(), st.Message())
				}
			} else {
				fmt.Printf("NodeId: %s, %s:%d Error: %s\n", pi.NodeId, hostStr, pi.Port, err)
			}
		}
	}
	if len(pss) == 0 {
		return
	}
	data, err := proto.Marshal(&pb.BatchProviderStatus{Ps: pss})
	if err != nil {
		panic(err)
	}
	en, err := util_aes.Encrypt(data, encryptKey)
	if err != nil {
		panic(err)
	}
	updateStatus(en)
}

func getProvider() ([]*pb.Provider, error) {
	conn, err := grpc.Dial(conf.ApiHostAndPort, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pb.NewCheckavAilabilityServiceClient(conn)
	return FindProvider(client)
}

func updateStatus(data []byte) {
	conn, err := grpc.Dial(conf.ApiHostAndPort, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewCheckavAilabilityServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		req := &pb.UpdateStatusReq{Timestamp: uint64(time.Now().Unix()),
			Locality: conf.Locality,
			Data:     data}
		req.GenAuth(authToken)
		_, err = client.UpdateStatus(ctx, req)
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.DeadlineExceeded {
				continue
			} else {
				panic(err)
			}
		}
	}
}

func FindProvider(client pb.CheckavAilabilityServiceClient) (ps []*pb.Provider, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.FindProviderReq{Timestamp: uint64(time.Now().Unix()),
		Locality: conf.Locality}
	req.GenAuth(authToken)
	resp, er := client.FindProvider(ctx, req)
	if er != nil {
		return nil, er
	}
	bs, er := util_aes.Decrypt(resp.Data, encryptKey)
	if er != nil {
		return nil, er
	}
	batch := &pb.BatchProvider{}
	if err = proto.Unmarshal(bs, batch); err != nil {
		return
	}
	return batch.P, nil
}

func checkProvider(hostStr string, port uint32, pubKey []byte) (total uint64, maxFileSize uint64, err error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", hostStr, port), grpc.WithInsecure())
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	psc := provider_pb.NewProviderServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := &provider_pb.CheckAvailableReq{Timestamp: uint64(time.Now().Unix())}
	req.GenAuth(pubKey)
	resp, err := psc.CheckAvailable(ctx, req)
	if err != nil {
		return 0, 0, err
	}
	return resp.Total, resp.MaxFileSize, nil
}

const config_filename = "config.toml"

type Config struct {
	CronExp        string `default:"0 */2 * * * *"`
	AuthToken      string `default:"test"`
	EncryptKeyHex  string `default:"4fcf16120e28dec237da6ecdcb7ec3be"`
	Locality       string `default:"cn-beijing-corp"`
	ApiHostAndPort string `default:"localhost:6633"`
}

func GetConfig() *Config {
	m := multiconfig.NewWithPath(config_filename) // supports TOML, JSON and YAML
	config := new(Config)
	err := m.Load(config) // Check for error
	if err != nil {
		fmt.Printf("GetConfig Error: %s\n", err)
	}
	m.MustLoad(config) // Panic's if there is any error
	//	fmt.Printf("%+v\n", config)
	return config
}
