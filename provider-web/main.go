package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"nebula-tracker/config"
	"nebula-tracker/db"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/koding/multiconfig"
	"github.com/robfig/cron"
	uuid "github.com/satori/go.uuid"

	log "github.com/sirupsen/logrus"
)

func main() {
	// defer func() {
	// 	if err := recover(); err != nil {
	// 		fmt.Printf("Panic Error: %s\n", err)
	// 		debug.PrintStack()
	// 		log.Println(string(debug.Stack()))
	// 	}
	// }()
	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	conf := GetConfig(path + string(os.PathSeparator) + "config.toml")
	dbo := db.OpenDb(&conf.Db)
	defer dbo.Close()

	var cronRunner = cron.New()
	cronRunner.AddFunc("0 15,45 * * * *", clearExpiredSession)
	defer cronRunner.Stop()
	http.HandleFunc("/", indexHandler)

	http.HandleFunc("/login/", loginHandler)
	http.HandleFunc("/logout/", logoutHandler)

	http.HandleFunc("/kyc/", indexHandler)

	http.Handle("/s/", cache(http.StripPrefix("/s/", http.FileServer(http.Dir("s")))))
	http.Handle("/i18n/", cache(http.StripPrefix("/i18n/", http.FileServer(http.Dir("i18n")))))

	http.HandleFunc("/favicon.ico", serveFileHandler)
	http.HandleFunc("/robots.txt", serveFileHandler)

	db.OpenDb(&conf.Db)
	defer db.CloseDb()

	s := &http.Server{
		Addr:           fmt.Sprintf("%s:%d", conf.ListenIp, conf.ListenPort),
		Handler:        http.DefaultServeMux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	loadSession()
	go func() {
		if err := s.ListenAndServe(); err != nil {
			log.Warnf("ListenAndServe Error： %s", err)
		}
		log.Info("server is shutdown")
	}()
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	if err := s.Shutdown(nil); err != nil {
		log.Warnf("Shutdown Error： %s", err)
	}
	storeSession()
}

func serveFileHandler(w http.ResponseWriter, r *http.Request) {
	fname := path.Base(r.URL.Path)
	w.Header().Set("Cache-Control", "max-age=604800") //7days
	http.ServeFile(w, r, "./s/"+fname)
}
func cache(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=7776000") //90days
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

type JsonObj struct {
	Code   uint8       `json:"code"`
	ErrMsg string      `json:"errmsg"`
	Data   interface{} `json:"data"`
}

// var templates = template.Must(template.ParseGlob("*.html"))
var templates = template.Must(template.New("").Funcs(template.FuncMap{"noEscapeUrl": noEscapeUrl}).ParseGlob("*.html"))

func noEscapeUrl(x string) interface{} { return template.URL(x) }

func renderTemplate(w http.ResponseWriter, tmpl string, data interface{}) {
	if os.Getenv("DEVELOP_MODE") == "1" {
		// temp := template.Must(template.ParseGlob(tmpl + ".html"))
		temp := template.Must(template.New("").Funcs(template.FuncMap{"noEscapeUrl": noEscapeUrl}).ParseGlob("*.html"))
		err := temp.ExecuteTemplate(w, tmpl+".html", data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	err := templates.ExecuteTemplate(w, tmpl+".html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type Client struct {
	Account     string
	LastRequest int64
}

// see: https://jacobmartins.com/2016/04/06/practical-golang-writing-a-simple-login-middleware/
var sessionStore map[string]*Client = make(map[string]*Client)
var storageMutex sync.RWMutex

const sessionStoreFilename = "session.store"

func storeSession() {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	storageMutex.RLock()
	defer storageMutex.RUnlock()
	if err := encoder.Encode(sessionStore); err != nil {
		log.Warnf("encode session store error: %s", err)
		return
	}
	if err := ioutil.WriteFile(sessionStoreFilename, buffer.Bytes(), 0600); err != nil {
		log.Warnf("write session store file %s error: %s", sessionStoreFilename, err)
		return
	}
}

func loadSession() { //data interface{}, filename string
	if _, err := os.Stat(sessionStoreFilename); os.IsNotExist(err) {
		return
	}
	raw, err := ioutil.ReadFile(sessionStoreFilename)
	if err != nil {
		log.Warnf("read session store file %s error: %s", sessionStoreFilename, err)
		return
	}
	buffer := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buffer)
	if err = dec.Decode(&sessionStore); err != nil {
		log.Warnf("decode session store error: %s", err)
		return
	}
}

func clearExpiredSession() {
	ts := time.Now().Unix()
	storageMutex.Lock()
	defer storageMutex.Unlock()
	for k, v := range sessionStore {
		if ts-v.LastRequest > session_expire {
			delete(sessionStore, k)
		}
	}
}

const session_cookie = "sid"
const session_expire = 1800

func getClient(w http.ResponseWriter, r *http.Request) *Client {
	cookie, err := r.Cookie(session_cookie)
	if err != nil && err != http.ErrNoCookie {
		http.Error(w, "read cookie error: "+err.Error(), http.StatusInternalServerError)
		return nil
	}
	var present bool
	var client *Client
	if cookie != nil {
		storageMutex.RLock()
		client, present = sessionStore[cookie.Value]
		storageMutex.RUnlock()
	} else {
		present = false
	}
	ts := time.Now().Unix()
	if present && ts-client.LastRequest <= session_expire {
		client.LastRequest = ts
	} else {
		if present && ts-client.LastRequest > session_expire {
			storageMutex.Lock()
			delete(sessionStore, cookie.Value)
			storageMutex.Unlock()
		}
		cookie = &http.Cookie{
			Name:     session_cookie,
			Value:    uuid.NewV4().String(),
			Path:     "/",
			HttpOnly: true,
		}
		client = &Client{LastRequest: ts}
		storageMutex.Lock()
		sessionStore[cookie.Value] = client
		storageMutex.Unlock()
		http.SetCookie(w, cookie)
	}
	return client
}

func sha1Str(str string) string {
	h := sha1.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
func loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		client := getClient(w, r)
		if client.Account != "" {
			http.Redirect(w, r, "/", 302)
			return
		}
		renderTemplate(w, "index", struct {
			Login  bool
			ErrMsg string
		}{true, ""})
		return
	} else if r.Method == "POST" {
		r.ParseForm()
		email := strings.TrimSpace(r.PostFormValue("billEmail"))
		nodeId := strings.TrimSpace(r.PostFormValue("nodeId"))
		errMsg := "bill email or node id information error"
		if len(email) > 0 && len(nodeId) > 0 {
			bts, err := hex.DecodeString(nodeId)
			if err == nil {
				nodeId = base64.StdEncoding.EncodeToString(bts)
				if db.FindProviderByEmailAndNodeId(email, nodeId) {
					client := getClient(w, r)
					client.Account = nodeId
					http.Redirect(w, r, "/", 302)
					return
				}
			}
		} else {
			errMsg = "both bill email or node id are required"
		}
		renderTemplate(w, "index", struct {
			Login  bool
			ErrMsg string
		}{true, errMsg})
	} else {
		http.Error(w, "unsupported method: "+r.Method, http.StatusInternalServerError)
	}
}

func logoutHandler(w http.ResponseWriter, r *http.Request) {
	client := getClient(w, r)
	if client.Account != "" {
		client.Account = ""
	}
	http.Redirect(w, r, "/login/", 302)
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
	Db          config.Db
	ListenIp    string `default:"127.0.0.1"`
	ListenPort  int    `default:"7000"`
	NaThreshold int    `default:"300"`
	Offset      int    `default:"60"`
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	client := getClient(w, r)
	if client.Account == "" {
		http.Redirect(w, r, "/login/", 302)
		return
	}
	providerId := client.Account
	slice := make([]DayStatus, 0, 6)
	t := time.Now().UTC()
	tm := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	for i := 0; i < 6; i++ {
		tm := tm.AddDate(0, 0, -1)
		day := tm.Format("2006-01-02")
		naSection := db.GetDailyNaByProviderAndDay(providerId, day)
		slice = append(slice, DayStatus{Date: day, NaSection: naSection, TotalNaMins: sumNaMins(naSection)})
	}
	found, last := db.GetLastCheckAvailRecord(providerId)
	online := false
	if found {
		if time.Now().Unix()-last.Unix() < 480 {
			online = true
		}
	}
	renderTemplate(w, "index", struct {
		Login     bool
		Online    bool
		Found     bool
		Last      time.Time
		DayStatus []DayStatus
	}{Login: false, Online: online, Found: found, Last: last, DayStatus: slice})
}

type DayStatus struct {
	Date        string
	NaSection   [][2]time.Time
	TotalNaMins int64
}

func sumNaMins(naSection [][2]time.Time) (sum int64) {
	for _, sec := range naSection {
		sum += (sec[1].Unix() - sec[0].Unix())
	}
	return sum / 60
}

func status(w http.ResponseWriter, r *http.Request) {
	client := getClient(w, r)
	if client.Account == "" {
		json.NewEncoder(w).Encode(&JsonObj{Code: 1, ErrMsg: "not login"})
		return
	}
	providerId := client.Account
	found, last := db.GetLastCheckAvailRecord(providerId)
	if !found {
		json.NewEncoder(w).Encode(&JsonObj{Data: struct{ Found bool }{Found: true}})
	} else {
		json.NewEncoder(w).Encode(&JsonObj{Data: struct {
			Found bool
			Last  int64
		}{Found: true, Last: last.Unix()}})
	}

}
