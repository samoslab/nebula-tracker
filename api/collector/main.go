package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"nebula-tracker/db"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"time"

	util_aes "github.com/samoslab/nebula/util/aes"
	log "github.com/sirupsen/logrus"

	"nebula-tracker/config"
)

var encryptKey []byte

func main() {
	// fmt.Println(randAesKey(16))
	http.HandleFunc("/api/pub-key/client_all/", clientAll)
	http.HandleFunc("/api/pub-key/provider_all/", providerAll)
	http.HandleFunc("/api/pub-key/client/", client)
	http.HandleFunc("/api/pub-key/provider/", provider)
	conf := config.GetInterfaceConfig()
	var err error
	encryptKey, err = hex.DecodeString(conf.EncryptKeyHex)
	if err != nil {
		log.Fatalf("decode encrypt key Error： %s", err)
	}
	if len(encryptKey) != 16 && len(encryptKey) != 24 && len(encryptKey) != 32 {
		log.Fatalf("encrypt key length Error： %d", len(encryptKey))
	}
	db.OpenDb(&conf.Db)
	defer db.CloseDb()
	fmt.Printf("Listening on %s:%d\n", conf.ListenIp, conf.ListenPort)
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.ListenIp, conf.ListenPort), nil)
	if err != nil {
		log.Fatalf("ListenAndServe Error： %s", err)
	}
}

type JsonObj struct {
	Code   uint8       `json:"code"`
	ErrMsg string      `json:"errmsg"`
	Data   interface{} `json:"data"`
}

func client(w http.ResponseWriter, r *http.Request) {
	defer recoverErr(w, r)
	if !chechAuthHeader(w, r) {
		return
	}
	nodeIdstr := r.RequestURI[len("/api/pub-key/client/"):]
	nodeId, pass := checkNodeId(w, nodeIdstr)
	if !pass {
		return
	}
	pubKey := db.ClientGetPubKeyBytesByNodeId(nodeId)
	if len(pubKey) == 0 {
		json.NewEncoder(w).Encode(&JsonObj{Code: 2, ErrMsg: "node id not exist: " + nodeId})
	}
	en, err := util_aes.Encrypt(pubKey, encryptKey)
	if err != nil {
		json.NewEncoder(w).Encode(&JsonObj{Code: 3, ErrMsg: "encrypt public key failed: " + err.Error()})
	} else {
		json.NewEncoder(w).Encode(&JsonObj{Data: base64.StdEncoding.EncodeToString(en)})
	}
}

func provider(w http.ResponseWriter, r *http.Request) {
	defer recoverErr(w, r)
	if !chechAuthHeader(w, r) {
		return
	}
	nodeIdstr := r.RequestURI[len("/api/pub-key/provider/"):]
	nodeId, pass := checkNodeId(w, nodeIdstr)
	if !pass {
		return
	}
	pubKey := db.ProviderGetPubKeyBytesByNodeId(nodeId)
	if len(pubKey) == 0 {
		json.NewEncoder(w).Encode(&JsonObj{Code: 2, ErrMsg: "node id not exist: " + nodeId})
	}
	en, err := util_aes.Encrypt(pubKey, encryptKey)
	if err != nil {
		json.NewEncoder(w).Encode(&JsonObj{Code: 3, ErrMsg: "encrypt public key failed: " + err.Error()})
	} else {
		json.NewEncoder(w).Encode(&JsonObj{Data: base64.StdEncoding.EncodeToString(en)})
	}
}

func processMap(w http.ResponseWriter, m map[string][]byte) {
	res := make([][]string, 0, len(m))
	var err error
	var bs []byte
	for k, v := range m {
		bs, err = util_aes.Encrypt(v, encryptKey)
		if err != nil {
			json.NewEncoder(w).Encode(&JsonObj{Code: 4, ErrMsg: "encrypt public key failed: " + err.Error()})
			return
		}
		res = append(res, []string{k, base64.StdEncoding.EncodeToString(bs)})
	}
	json.NewEncoder(w).Encode(&JsonObj{Data: res})
}

func clientAll(w http.ResponseWriter, r *http.Request) {
	defer recoverErr(w, r)
	if !chechAuthHeader(w, r) {
		return
	}
	processMap(w, db.ClientAllPubKeyBytes())
}

func providerAll(w http.ResponseWriter, r *http.Request) {
	defer recoverErr(w, r)
	if !chechAuthHeader(w, r) {
		return
	}
	processMap(w, db.ProviderAllPubKeyBytes())
}

func checkNodeId(w http.ResponseWriter, nodeIdStr string) (nodeId string, pass bool) {
	if len(nodeIdStr) == 0 {
		json.NewEncoder(w).Encode(&JsonObj{Code: 11, ErrMsg: "node id is required."})
		return "", false
	}
	var err error
	nodeIdStr, err = url.QueryUnescape(nodeIdStr)
	if err != nil {
		json.NewEncoder(w).Encode(&JsonObj{Code: 12, ErrMsg: fmt.Sprintf("unescape node id [%s] failed: %v", nodeIdStr, err)})
		return "", false
	}
	// nodeId, err = base64.StdEncoding.DecodeString(nodeIdStr)
	// if err != nil {
	// 	json.NewEncoder(w).Encode(&JsonObj{Code: 13, ErrMsg: fmt.Sprintf("base64 decode node id [%s] failed: %v", nodeIdStr, err)})
	// 	return nil, false
	// }
	return nodeId, true
}

func chechAuthHeader(w http.ResponseWriter, r *http.Request) bool {
	//	if true {
	//		return true
	//	}
	conf := config.GetInterfaceConfig()
	tsStr := r.Header.Get("timestamp")
	ts, err := strconv.Atoi(tsStr)
	if err != nil {
		json.NewEncoder(w).Encode(&JsonObj{Code: 22, ErrMsg: "invalid header timestamp: " + err.Error()})
		return false
	}
	timestamp := int64(ts)
	current := time.Now().Unix()
	if timestamp-current > 3 {
		json.NewEncoder(w).Encode(&JsonObj{Code: 23, ErrMsg: "client time error"})
		return false
	}
	if current-timestamp > int64(conf.AuthValidSec) {
		json.NewEncoder(w).Encode(&JsonObj{Code: 24, ErrMsg: "timestamp expired"})
		return false
	}
	hash := hmac.New(sha256.New, []byte(conf.AuthToken))
	hash.Write([]byte(tsStr))
	if hex.EncodeToString(hash.Sum(nil)) != r.Header.Get("auth") {
		json.NewEncoder(w).Encode(&JsonObj{Code: 25, ErrMsg: "auth verify error"})
		return false
	}
	return true
}

func success(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(&JsonObj{})
}

func recoverErr(w http.ResponseWriter, r *http.Request) {
	if err := recover(); err != nil {
		debug.PrintStack()
		log.Println(string(debug.Stack()))
		json.NewEncoder(w).Encode(&JsonObj{Code: 10, ErrMsg: fmt.Sprint(err)})
	}
}

func randAesKey(bits int) string {
	token := make([]byte, bits)
	_, err := rand.Read(token)
	if err != nil {
		log.Errorf("generate AES key err: %s", err)
	}
	return hex.EncodeToString(token)
}
