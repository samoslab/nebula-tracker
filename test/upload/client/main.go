package main

import (
	"context"
	"fmt"
	"io"
	"nebula-tracker/test/upload/server/pb"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func upload(client pb.UploadServiceClient, batchSize uint32, filePath string) error {
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()
	ctx := context.Background()
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	stream, err := client.Upload(ctx)
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
		if err := stream.Send(&pb.UploadReq{Name: filepath.Base(filePath), Data: buf[:bytesRead]}); err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		if uint32(bytesRead) < batchSize {
			break
		}
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		st, ok := status.FromError(err)
		if !ok {
			fmt.Printf("CloseAndRecv error: %s\n", err)
			return err
		} else {
			if st.Code() == codes.AlreadyExists {
				fmt.Printf("AlreadyExists error: %s\n", err)
				return nil
			}
			return err
		}
	}
	return nil
}

func download(client pb.UploadServiceClient, batchSize uint32, filename string) error {
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()
	ctx := context.Background()
	stream, err := client.Download(ctx, &pb.DownloadReq{Name: filename, BatchSize: batchSize})
	if err != nil {
		return err
	}
	var file *os.File
	first := true
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Recv error: " + err.Error())
			return err
		}
		if len(resp.Data) == 0 {
			break
		}
		if first {
			first = false
			file, err = os.OpenFile(
				filename,
				os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
				0600)
			if err != nil {
				return err
			}
			defer file.Close()
		}
		if _, err = file.Write(resp.Data); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 5 {
		fmt.Printf("Error Usage. arg count: %d\n", len(os.Args))
		return
	}
	val, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}
	batchSize := uint32(val)
	filePathOrFilename := os.Args[4]
	conn, err := grpc.Dial(os.Args[1], grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	usc := pb.NewUploadServiceClient(conn)
	start := time.Now().UnixNano()
	switch os.Args[2] {
	case "upload":
		err = upload(usc, batchSize, filePathOrFilename)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%q batchSize: %d cost: %d\n", os.Args[2], batchSize, time.Now().UnixNano()-start)
	case "download":
		err = download(usc, batchSize, filePathOrFilename)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%q batchSize: %d cost: %d\n", os.Args[2], batchSize, time.Now().UnixNano()-start)
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[2])
		os.Exit(2)
	}
}
