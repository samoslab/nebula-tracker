package main

import (
	"fmt"
	"nebula-tracker/db"
	"net/http"

	"nebula-tracker/config"
)

func main() {
	http.HandleFunc("/api/all-public-key-of-provider/", allPublicKey)
	http.HandleFunc("/api/public-key-of-provider/", publicKey)
	conf := config.GetInterfaceConfig()
	db.OpenDb(&conf.Db)
	defer db.CloseDb()
	fmt.Printf("Listening on %s:%d\n", conf.ListenIp, conf.ListenPort)
	err := http.ListenAndServe(fmt.Sprintf("%s:%d", conf.ListenIp, conf.ListenPort), nil)
	if err != nil {
		println("ListenAndServe Error： %s", err)
	}
}

type JsonObj struct {
	Code   uint8       `json:"code"`
	ErrMsg string      `json:"errmsg"`
	Data   interface{} `json:"data"`
}

func allPublicKey(w http.ResponseWriter, r *http.Request) {
}

func publicKey(w http.ResponseWriter, r *http.Request) {
}
