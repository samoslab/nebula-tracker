package tracker_client

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"nebula-tracker/collector/config"

	util_aes "github.com/samoslab/nebula/util/aes"
	log "github.com/sirupsen/logrus"
)

type jsonResp struct {
	Code   int16           `json:"code"`
	ErrMsg string          `json:"errmsg"`
	Data   json.RawMessage `json:"data"`
}

var encryptKey []byte

func init() {
	var err error
	encryptKey, err = hex.DecodeString(config.GetConsumerConfig().TrackerInterface.EncryptKeyHex)
	if err != nil {
		log.Fatalf("decode encrypt key Error： %s", err)
	}
	if len(encryptKey) != 16 && len(encryptKey) != 24 && len(encryptKey) != 32 {
		log.Fatalf("encrypt key length Error： %d", len(encryptKey))
	}
}
func setAuthHeaders(req *http.Request, ti *config.TrackerInterface) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	hash := hmac.New(sha256.New, []byte(ti.ApiToken))
	hash.Write([]byte(timestamp))
	req.Header.Set("timestamp", timestamp)
	req.Header.Set("auth", hex.EncodeToString(hash.Sum(nil)))
	req.Header.Set("client", "collector")
}

func httpGet(url string) ([]byte, error) {
	ti := config.GetConsumerConfig().TrackerInterface
	client := &http.Client{}
	req, err := http.NewRequest("GET", ti.ContextPath+url, nil)
	if err != nil {
		return nil, err
	}

	setAuthHeaders(req, &ti)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if ti.Debug {
		fmt.Println(string(body))
	}
	return body, nil
}

func getSingle(nodeId string, subPath string) (pubKey []byte, err error) {
	resp, err := httpGet(subPath + "?nodeId=" + url.QueryEscape(nodeId))
	if err != nil {
		return nil, err
	}
	jsonObj := new(jsonResp)
	err = json.Unmarshal(resp, &jsonObj)
	if err != nil {
		return nil, err
	}
	if jsonObj.Code != 0 {
		if jsonObj.Code == 2 {
			return nil, nil
		}
		return nil, fmt.Errorf("%s code:%d", jsonObj.ErrMsg, jsonObj.Code)
	}
	var str string
	err = json.Unmarshal(jsonObj.Data, &str)
	if err != nil {
		return nil, err
	}
	en, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, err
	}
	return util_aes.Decrypt(en, encryptKey)
}

func getAll(subPath string) (map[string][]byte, error) {
	resp, err := httpGet(subPath)
	if err != nil {
		return nil, err
	}
	jsonObj := new(jsonResp)
	err = json.Unmarshal(resp, &jsonObj)
	if err != nil {
		return nil, err
	}
	if jsonObj.Code != 0 {
		return nil, fmt.Errorf("%s code:%d", jsonObj.ErrMsg, jsonObj.Code)
	}
	body := make([][]string, 0, 16)
	if err = json.Unmarshal(jsonObj.Data, &body); err != nil {
		return nil, err
	}
	m := make(map[string][]byte, len(body))
	for _, arr := range body {
		if len(arr) != 2 {
			panic("data structure error")
		}
		bs, err := base64.StdEncoding.DecodeString(arr[1])
		if err != nil {
			return nil, fmt.Errorf("decode base64 string failed: " + err.Error())
		}
		bs, err = util_aes.Decrypt(bs, encryptKey)
		if err != nil {
			return nil, fmt.Errorf("decrypt public key failed: " + err.Error())
		}
		m[arr[0]] = bs
	}
	return m, nil
}

func ClientPubKey(nodeId string) ([]byte, error) {
	return getSingle(nodeId, "/pub-key/client/")
}

func ClientAllPubKey() (map[string][]byte, error) {
	return getAll("/pub-key/client_all/")
}

func ProviderPubKey(nodeId string) ([]byte, error) {
	return getSingle(nodeId, "/pub-key/provider/")
}

func ProviderAllPubKey() (map[string][]byte, error) {
	return getAll("/pub-key/provider_all/")
}
