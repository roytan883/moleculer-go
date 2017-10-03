package main

import (
	"flag"
	"os"
	"os/signal"
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
		// os.Interrupt,
		syscall.SIGINT,
		syscall.SIGHUP,
		syscall.SIGKILL,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	// This goroutine executes a blocking receive for
	// signals. When it gets one it'll print it out
	// and then notify the program that it can finish.
	go func() {
		sig := <-sigs
		log.Info("on system signal: ", sig)
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

func createDemoService() moleculer.Service {
	service := moleculer.Service{
		ServiceName: "demoService",
		Actions:     make(map[string]moleculer.RequestHandler),
		Events:      make(map[string]moleculer.EventHandler),
	}

	//init actions handlers
	actionA := func(req *protocol.MsRequest) (interface{}, error) {
		log.Info("run actionA, req.Params = ", req.Params)
		data := map[string]interface{}{
			"res1": "AAA",
			"res2": 123,
		}
		return data, nil
		// return nil, errors.New("test return error in actionA")
	}
	actionB := func(req *protocol.MsRequest) (interface{}, error) {
		log.Info("run actionB, req.Params = ", req.Params)
		data := map[string]interface{}{
			"res1": "BBB",
			"res2": 456,
		}
		return data, nil
		// return nil, errors.New("test return error in actionB")
	}
	bench := func(req *protocol.MsRequest) (interface{}, error) {
		// log.Info("run actionB, req.Params = ", req.Params)
		data := map[string]interface{}{
			"res1": "CCC",
			"res2": 789,
		}
		return data, nil
		// return nil, errors.New("test return error in actionB")
	}
	service.Actions["actionA"] = actionA
	service.Actions["actionB"] = actionB
	service.Actions["bench"] = bench

	//init listen events handlers
	onEventUserCreate := func(req *protocol.MsEvent) {
		log.Info("run onEventUserCreate, req.Data = ", req.Data)
	}
	onEventUserDelete := func(req *protocol.MsEvent) {
		log.Info("run onEventUserDelete, req.Data = ", req.Data)
	}
	service.Events["user.create"] = onEventUserCreate
	service.Events["user.delete"] = onEventUserDelete

	return service
}

//go run .\examples\moleculer-go-demo.go -s nats://192.168.1.69:12008
func main() {

	//get NATS server host
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	flag.Usage = usage
	flag.Parse()
	var hosts = strings.Split(*urls, ",")
	log.Printf("hosts : '%v'\n", hosts)

	//init service and broker
	config := &moleculer.ServiceBrokerConfig{
		NatsHost: hosts,
		NodeID:   "moleculer-go-demo",
		// LogLevel: moleculer.DebugLevel,
		LogLevel: moleculer.ErrorLevel,
		Services: make(map[string]moleculer.Service),
	}
	config.Services["demoService"] = createDemoService()
	broker, err := moleculer.NewServiceBroker(config)
	if err != nil {
		log.Fatalf("NewServiceBroker err: %v\n", err)
	}
	pBroker = broker
	broker.Start()

	//test call and emit
	go time.AfterFunc(time.Second*1, func() {
		log.Info("broker.Call demoService.actionA start")
		res, err := broker.Call("demoService.actionA", map[string]interface{}{
			"arg1": "aaa",
			"arg2": 123,
		}, nil)
		log.Info("broker.Call demoService.actionA end, res: ", res)
		log.Info("broker.Call demoService.actionA end, err: ", err)

		log.Info("broker.Call demoService.actionB start")
		res, err = broker.Call("demoService.actionB", map[string]interface{}{
			"arg1": "bbb",
			"arg2": 456,
		}, nil)
		log.Info("broker.Call demoService.actionB end, res: ", res)
		log.Info("broker.Call demoService.actionB end, err: ", err)

		log.Info("broker.Emit user.create start")
		err = broker.Emit("user.create", map[string]interface{}{
			"user":   "userA",
			"status": "create",
		})
		log.Info("broker.Emit user.create end, err: ", err)

		log.Info("broker.Broadcast user.delete start")
		err = broker.Broadcast("user.delete", map[string]interface{}{
			"user":   "userB",
			"status": "delete",
		})
		log.Info("broker.Broadcast user.delete end, err: ", err)

	})

	waitExit()

}
