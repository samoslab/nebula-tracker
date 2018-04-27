package main

import (
	"context"
	"fmt"
	"io"
	"nebula-tracker/test/upload/server/pb"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
)

func upload(client pb.UploadServiceClient, batchSize uint32) error {
	file, err := os.Open("test.file")
	if err != nil {
		return err
	}
	defer file.Close()
	stream, err := client.Upload(context.Background())
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	buf := make([]byte, batchSize)
	for {
		bytesRead, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := stream.Send(&pb.UploadReq{Data: buf[:bytesRead]}); err != nil {
			return nil
		}
		if uint32(bytesRead) < batchSize {
			break
		}
	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		return err
	}
	return nil
}

func download(client pb.UploadServiceClient, batchSize uint32) error {
	file, err := os.OpenFile(
		strconv.FormatInt(time.Now().Unix(), 16)+".file",
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0600)
	if err != nil {
		return err
	}
	defer file.Close()
	stream, err := client.Download(context.Background(), &pb.DownloadReq{BatchSize: batchSize})
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(resp.Data) == 0 {
			break
		}
		if _, err = file.Write(resp.Data); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Error Usage.")
		return
	}
	val, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}
	batchSize := uint32(val)
	conn, err := grpc.Dial("127.0.0.1:6666", grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	usc := pb.NewUploadServiceClient(conn)
	start := time.Now().UnixNano()
	switch os.Args[1] {
	case "upload":
		upload(usc, batchSize)
		fmt.Printf("%q cost: %d\n", os.Args[1], time.Now().UnixNano()-start)
	case "download":
		download(usc, batchSize)
		fmt.Printf("%q cost: %d\n", os.Args[1], time.Now().UnixNano()-start)
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(2)
	}
}
