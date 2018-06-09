package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"nebula-tracker/config"
	"nebula-tracker/db"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {

	http.HandleFunc("/api/count-available-address/", countAvailableAddress)
	http.HandleFunc("/api/address/", addressHandler)
	http.HandleFunc("/api/deposit/", depositHandler)

	conf := config.GetApiForTellerConfig()
	db.OpenDb(&conf.Db)
	defer db.CloseDb()
	fmt.Printf("Listening on %s:%d\n", conf.ListenIp, conf.ListenPort)
	err := http.ListenAndServe(fmt.Sprintf("%s:%d", conf.ListenIp, conf.ListenPort), nil)
	if err != nil {
		println("ListenAndServe Error： %s", err)
	}
}

func countAvailableAddress(w http.ResponseWriter, r *http.Request) {
	defer recoverErr(w, r)
	if !chechAuthHeader(w, r) {
		return
	}
	json.NewEncoder(w).Encode(&JsonObj{0, "", db.CountAvailableAddress()})
}

func addressHandler(w http.ResponseWriter, r *http.Request) {
	defer recoverErr(w, r)
	if !chechAuthHeader(w, r) {
		return
	}
	addrs := make([]*db.PreparedAddress, 0, 100)
	err := json.NewDecoder(r.Body).Decode(&addrs)
	if !checkJsonErr(err, w, r) {
		return
	}
	db.AddAvailableAddress(addrs)
	success(w, r)
}

func depositHandler(w http.ResponseWriter, r *http.Request) {
	defer recoverErr(w, r)
	if !chechAuthHeader(w, r) {
		return
	}
	drs := make([]*db.DepositRecord, 0, 8)
	err := json.NewDecoder(r.Body).Decode(&drs)
	if !checkJsonErr(err, w, r) {
		return
	}
	db.SaveDepositRecord(drs)
	success(w, r)
}

type JsonObj struct {
	Code   int8        `json:"code"`
	ErrMsg string      `json:"errmsg"`
	Data   interface{} `json:"data"`
}

func recoverErr(w http.ResponseWriter, r *http.Request) {
	if err := recover(); err != nil {
		debug.PrintStack()
		log.Warn(string(debug.Stack()))
		json.NewEncoder(w).Encode(&JsonObj{Code: 9, ErrMsg: fmt.Sprint(err)})
	}
}

func checkJsonErr(err error, w http.ResponseWriter, r *http.Request) bool {
	if err != nil {
		json.NewEncoder(w).Encode(&JsonObj{Code: 1, ErrMsg: "cannot parse request to JSON:" + err.Error()})
		return false
	}
	return true
}
func chechAuthHeader(w http.ResponseWriter, r *http.Request) bool {
	//	if true {
	//		return true
	//	}
	conf := config.GetApiForTellerConfig()
	tsStr := r.Header.Get("timestamp")
	ts, err := strconv.Atoi(tsStr)
	if err != nil {
		json.NewEncoder(w).Encode(&JsonObj{Code: 2, ErrMsg: "invalid header timestamp: " + err.Error()})
		return false
	}
	timestamp := int64(ts)
	current := time.Now().Unix()
	if timestamp-current > 3 {
		json.NewEncoder(w).Encode(&JsonObj{Code: 3, ErrMsg: "client time error"})
		return false
	}
	if current-timestamp > int64(conf.AuthValidSec) {
		json.NewEncoder(w).Encode(&JsonObj{Code: 4, ErrMsg: "timestamp expired"})
		return false
	}
	hash := hmac.New(sha256.New, []byte(conf.AuthToken))
	hash.Write([]byte(tsStr))
	if hex.EncodeToString(hash.Sum(nil)) != r.Header.Get("auth") {
		json.NewEncoder(w).Encode(&JsonObj{Code: 5, ErrMsg: "auth verify error"})
		return false
	}
	return true
}

func success(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(&JsonObj{})
}
