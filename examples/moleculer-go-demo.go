package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	logrus "github.com/Sirupsen/logrus"

	nats "github.com/nats-io/go-nats"
	moleculer "github.com/roytan883/moleculer-go"
	"github.com/roytan883/moleculer-go/protocol"
)

func init() {
	initLog()
}

var log *logrus.Logger

func initLog() {
	log = logrus.New()
	log.Formatter = &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "01-02 15:04:05.000000",
	}
	log.WithFields(logrus.Fields{"package": "main", "file": "moleculer-go-demo"})
}

var pBroker *moleculer.ServiceBroker

func waitExit() {
	// Go signal notification works by sending `os.Signal`
	// values on a channel. We'll create a channel to
	// receive these notifications (we'll also make one to
	// notify us when the program can exit).
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	// `signal.Notify` registers the given channel to
	// receive notifications of the specified signals.
	signal.Notify(sigs,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	// This goroutine executes a blocking receive for
	// signals. When it gets one it'll print it out
	// and then notify the program that it can finish.
	go func() {
		sig := <-sigs
		log.Debug(sig)
		done <- true
	}()
	// The program will wait here until it gets the
	// expected signal (as indicated by the goroutine
	// above sending a value on `done`) and then exit.
	log.Info("waitExit for SIGINT/SIGTERM ...")
	<-done

	log.Warn("=================== exit start =================== ")
	if pBroker != nil {
		pBroker.Stop()
	}
	time.Sleep(time.Second * 1)
	log.Warn("=================== exit end   =================== ")
	os.Exit(0)
}

// NOTE: Use tls scheme for TLS, e.g. nats-req -s tls://demo.nats.io:4443 foo hello
func usage() {
	log.Fatalf("Usage: moleculer-go-demo [-s server (%s)] \n", nats.DefaultURL)
}

func fnAAA(req *protocol.MsRequest) (interface{}, error) {
	log.Info("call AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	data := map[string]interface{}{
		"a": 111,
		"b": "abc",
		"c": true,
	}
	return data, nil
	// return nil, errors.New("test return error")
}

func onEventBBB(req *protocol.MsEvent) {
	log.Info("call onEventBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
}

//go run .\examples\moleculer-go-demo.go -s nats://192.168.1.69:12008
func main() {
	log.Info("CPU = ", runtime.NumCPU())

	// return
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")

	// log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	// args := flag.Args()
	// if len(args) < 2 {
	// 	usage()
	// }

	var hosts = strings.Split(*urls, ",")
	log.Printf("hosts : '%v'\n", hosts)

	service := moleculer.Service{
		ServiceName: "demo",
		Actions:     make(map[string]moleculer.RequestHandler),
		Events:      make(map[string]moleculer.EventHandler),
	}
	service.Actions["fnAAA"] = fnAAA
	service.Events["demo.eventB"] = onEventBBB

	config := &moleculer.ServiceBrokerConfig{
		NatsHost: hosts,
		NodeID:   "moleculer-go-demo",
		LogLevel: logrus.DebugLevel,
		Services: make(map[string]moleculer.Service),
	}
	config.Services["demo"] = service

	broker, err := moleculer.NewServiceBroker(config)
	if err != nil {
		log.Fatalf("NewServiceBroker err: %v\n", err)
	}
	pBroker = broker
	broker.Start()

	go time.AfterFunc(time.Second*3, func() {
		// res, err := broker.Call("pushConnector.test1", map[string]interface{}{
		// 	"a": 111,
		// 	"b": "abc",
		// 	"c": true,
		// }, nil)
		// log.Info("broker.Call res: ", res)
		// log.Info("broker.Call err: ", err)

		// res2, err2 := broker.Call("demo.fnAAA", map[string]interface{}{
		// 	"a": 111,
		// 	"b": "abc",
		// 	"c": true,
		// }, nil)
		// log.Info("broker.Call res2: ", res2)
		// log.Info("broker.Call err2: ", err2)

		err := broker.Emit("demo.eventB", map[string]interface{}{
			"a": 111,
			"b": "abc",
			"c": true,
		})
		log.Info("broker.Emit err: ", err)

		err = broker.Broadcast("demo.eventB", map[string]interface{}{
			"a": 333,
			"b": "def",
		})
		log.Info("broker.Emit err: ", err)

	})

	// go func() {
	// 	for {
	// 		select {
	// 		case <-time.After(time.Second * 3):
	// 			res3, err3 := broker.Call("demo.fnAAA", map[string]interface{}{
	// 				// res3, err3 := broker.Call("pushConnector.test1", map[string]interface{}{
	// 				"a": 111,
	// 				"b": "abc",
	// 				"c": true,
	// 			}, nil)
	// 			log.Info("broker.Call res3: ", res3)
	// 			log.Info("broker.Call err3: ", err3)
	// 		}
	// 	}
	// }()

	waitExit()

}
