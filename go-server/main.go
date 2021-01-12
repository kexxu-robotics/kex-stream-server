package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/crypto/acme/autocert"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/kexxu-robotics/kex-stream-server/go-server/eventstream"
)

// example dev certs:
//mkdir -p certs
//openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
//	-keyout certs/localhost.key -out certs/localhost.crt \
//	-subj "/C=NL/ST=Limburg/L=Geleen/O=Marco Franssen/OU=Development/CN=localhost/emailAddress=marco.franssen@gmail.com"

type Conf struct {
	Port          int
	Domain        string
	BaseUrl       string
	StaticPath    string
	ApiPass       string
	EventStreamId string

	Postgres struct {
		Host     string
		Db       string
		User     string
		Password string
	}

	Mqtt struct {
		Enabled          bool
		Username         string
		Password         string
		ClientId         string
		Servers          []string
		ServersSecure    []string
		WebServers       []string
		WebServersSecure []string
	}
}

func getLetsEncryptCert(certManager *autocert.Manager) func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		dirCache, ok := certManager.Cache.(autocert.DirCache)
		if !ok {
			dirCache = "certs"
		}

		keyFile := filepath.Join(string(dirCache), hello.ServerName)
		crtFile := filepath.Join(string(dirCache), hello.ServerName)
		certificate, err := tls.LoadX509KeyPair(crtFile, keyFile)
		if err != nil {
			fmt.Printf("%s\nFalling back to Letsencrypt\n", err)
			return certManager.GetCertificate(hello)
		}
		//fmt.Println("Loaded selfsigned certificate.")
		return &certificate, err
	}
}

type justFilesFilesystem struct {
	fs http.FileSystem
}

func (fs justFilesFilesystem) Open(name string) (http.File, error) {
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	return neuteredReaddirFile{f}, nil
}

type neuteredReaddirFile struct {
	http.File
}

func (f neuteredReaddirFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, nil
}

func checkAuthorized(wrapped http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.FormValue("pass") == conf.ApiPass {
			wrapped(w, r)
			return
		}
		http.Error(w, "access denied", http.StatusUnauthorized)
		time.Sleep(time.Second)
	}
}

func checkAuthorizedHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.FormValue("pass") == conf.ApiPass {
			h.ServeHTTP(w, r)
			return
		}
		http.Error(w, "access denied", http.StatusUnauthorized)
		time.Sleep(time.Second)
	})
}

var mqttDefaultPublish mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("MQTT TOPIC: %s MSG: %s\n", msg.Topic(), msg.Payload())
}

var conf Conf

func main() {
	fmt.Println("Kexxu Event Streaming Server")

	// load conf
	data, err := ioutil.ReadFile("conf.yaml")
	if err != nil {
		panic(err)
	}
	conf = Conf{}
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		panic(err)
	}
	fmt.Println("loaded conf", conf)
	if conf.StaticPath == "" {
		panic("ERROR! you cannot continue with static path not set to a subfolder, else you will be exposing valuable system files")
	}

	// connect to
	dbUrl := fmt.Sprint(conf.Postgres.Host, conf.Postgres.Db, "?user=", conf.Postgres.User, "&password=", conf.Postgres.Password)
	conn, err := pgxpool.Connect(context.Background(), dbUrl)
	if err != nil {
		panic(fmt.Sprintln("ERROR! cannot connect to Postgresql", err))
	}
	defer conn.Close()

	// test postgres connection
	var postgresTest string
	err = conn.QueryRow(context.Background(), "select 'Postgres connected'").Scan(&postgresTest)
	if err != nil {
		panic(fmt.Sprintln("ERROR! test call to Postgresql failed", err))
	}
	fmt.Println(postgresTest)

	// tests to check the server is running
	mux := http.NewServeMux()
	mux.HandleFunc("/api/test", checkAuthorized(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "OK")
	}))
	// test to check the database connection is working
	mux.HandleFunc("/api/testDb", checkAuthorized(func(w http.ResponseWriter, r *http.Request) {
		var postgresTest string
		err = conn.QueryRow(context.Background(), "select 'OK'").Scan(&postgresTest)
		if err != nil {
			fmt.Fprint(w, "ERROR! test call to Postgresql failed")
			return
		}
		fmt.Fprint(w, "OK")
	}))

	// init eventStream
	eventStream := eventstream.EventStream{
		Conn:          conn,
		EventStreamId: conf.EventStreamId,
	}

	// init mqtt
	if conf.Mqtt.Enabled {
		// https://github.com/eclipse/paho.mqtt.golang/blob/master/cmd/simple/main.go
		//mqtt.DEBUG = log.New(os.Stdout, "", 0)
		mqtt.ERROR = log.New(os.Stdout, "", 0)
		opts := mqtt.NewClientOptions().AddBroker("tcp://" + conf.Mqtt.Servers[0]).SetClientID(conf.Mqtt.ClientId)
		opts.SetUsername(conf.Mqtt.Username)
		opts.SetPassword(conf.Mqtt.Password)
		opts.SetKeepAlive(10 * time.Minute)
		opts.SetDefaultPublishHandler(mqttDefaultPublish)
		opts.SetPingTimeout(1 * time.Second)

		mqttClient := mqtt.NewClient(opts)
		if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}

		eventStream.MqttClient = &mqttClient

		// test to check the mqtt connection is working
		mux.HandleFunc("/api/testMqtt", checkAuthorized(func(w http.ResponseWriter, r *http.Request) {
			err := mqttClient.Publish("/test", 1, false, fmt.Sprint(time.Now()))
			if err != nil {
				fmt.Fprint(w, "ERROR! test call to Mqtt failed")
				return
			}
			fmt.Fprint(w, "OK")
		}))
	}

	// debug
	//if token := mqttClient.Subscribe("eventstream/#", 2, mqttDefaultPublish); token.Wait() && token.Error() != nil {
	//	fmt.Println(token.Error())
	//}

	// init origin security
	// this prevents origins spamming the service
	// and unknown origins from making requests
	originSecure := eventstream.Secure{
		Conn:              conn,
		MaxRequestsPerMin: 60,
	}
	go originSecure.ReloadOriginsChron()

	// init device handler
	eventsHandler := eventstream.Handler{
		BaseUrl:     conf.BaseUrl,
		StaticPath:  conf.StaticPath,
		Conn:        conn,
		Secure:      &originSecure,
		EventStream: &eventStream,
	}

	// eventstream endpoints
	mux.HandleFunc("/api/eventstream/addEvent", eventsHandler.AddEvent)
	mux.HandleFunc("/api/eventstream/getOriginEvents", eventsHandler.GetOriginEvents) // id, newestId (optional, to cap below id), lastId (optional, for pagination), limit (hard limit set at 10k)

	// mqtt very basic initial implementation
	mux.HandleFunc("/api/mqtt/server", checkAuthorized(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(conf.Mqtt.Servers[0]))
	}))

	mux.Handle("/", checkAuthorizedHandler(http.FileServer(justFilesFilesystem{http.Dir(conf.StaticPath)})))

	fmt.Println("TLS domain", conf.Domain)
	certManager := autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(conf.Domain),
		Cache:      autocert.DirCache("certs"),
	}

	if conf.Port == 443 {
		fmt.Println("using SSL protection")

		tlsConfig := certManager.TLSConfig()
		tlsConfig.GetCertificate = getLetsEncryptCert(&certManager)
		server := http.Server{
			Addr:      ":443",
			Handler:   mux,
			TLSConfig: tlsConfig,
		}

		//go http.ListenAndServe(":80", certManager.HTTPHandler(nil)) // only allow https for POST

		// also allow http
		go func() {
			httpServer := http.Server{
				Addr:    fmt.Sprint(":80"),
				Handler: mux,
			}
			if err := httpServer.ListenAndServe(); err != nil {
				panic(err)
			}
		}()

		// https server
		fmt.Println("Server listening on", server.Addr)
		if err := server.ListenAndServeTLS("", ""); err != nil {
			panic(err)
		}
	} else {
		server := http.Server{
			Addr:    fmt.Sprint(":", conf.Port),
			Handler: mux,
		}
		if err := server.ListenAndServe(); err != nil {
			fmt.Println(err)
		}
	}

}
