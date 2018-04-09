package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	pbc "github.com/samoslab/nebula/tracker/register/client/pb"
	pbp "github.com/samoslab/nebula/tracker/register/provider/pb"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("127.0.0.1:6666", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("RPC Dial failed: %s", err.Error())
		return
	}
	defer conn.Close()
	prsc := pbp.NewProviderRegisterServiceClient(conn)
	fmt.Println("==========test GetPublicKey for Provider RPC==========")
	res, err := GetPublicKeyForProvider(prsc)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(base64.StdEncoding.EncodeToString(res))
	}
	crsc := pbc.NewClientRegisterServiceClient(conn)
	fmt.Println("==========test GetPublicKey for Client RPC==========")
	res2, err := GetPublicKeyForClient(crsc)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(base64.StdEncoding.EncodeToString(res2))
	}
}

func GetPublicKeyForProvider(client pbp.ProviderRegisterServiceClient) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.GetPublicKey(ctx, &pbp.GetPublicKeyReq{})
	if err != nil {
		return nil, err
	}
	return resp.PublicKey, nil
}

func GetPublicKeyForClient(client pbc.ClientRegisterServiceClient) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.GetPublicKey(ctx, &pbc.GetPublicKeyReq{})
	if err != nil {
		return nil, err
	}
	return resp.PublicKey, nil
}
