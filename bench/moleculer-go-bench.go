package main

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	logrus "github.com/sirupsen/logrus"

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
	// log.WithFields(logrus.Fields{"package": "main", "file": "moleculer-go-demo"})
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
	log.Fatalf("Usage: moleculer-go-demo [-s server (%s)] [-i nodeId (0)] [-m MethodName (benchService.bench)] [-n Client number (5)] [-c each client call method count (10000)]\n", nats.DefaultURL)
}

func createDemoService() moleculer.Service {
	service := moleculer.Service{
		ServiceName: ServiceName,
		Actions:     make(map[string]moleculer.RequestHandler),
		Events:      make(map[string]moleculer.EventHandler),
	}

	//init actions handlers
	// actionA := func(req *protocol.MsRequest) (interface{}, error) {
	// 	log.Info("run actionA, req.Params = ", req.Params)
	// 	data := map[string]interface{}{
	// 		"res1": "AAA",
	// 		"res2": 123,
	// 	}
	// 	return data, nil
	// 	// return nil, errors.New("test return error in actionA")
	// }
	// actionB := func(req *protocol.MsRequest) (interface{}, error) {
	// 	log.Info("run actionB, req.Params = ", req.Params)
	// 	data := map[string]interface{}{
	// 		"res1": "BBB",
	// 		"res2": 456,
	// 	}
	// 	return data, nil
	// 	// return nil, errors.New("test return error in actionB")
	// }
	bench := func(req *protocol.MsRequest) (interface{}, error) {
		// log.Info("run actionB, req.Params = ", req.Params)
		data := map[string]interface{}{
			"res1": "CCC",
			"res2": 789,
		}
		return data, nil
		// return nil, errors.New("test return error in actionB")
	}
	// service.Actions["actionA"] = actionA
	// service.Actions["actionB"] = actionB
	service.Actions["bench"] = bench

	//init listen events handlers
	// onEventUserCreate := func(req *protocol.MsEvent) {
	// 	log.Info("run onEventUserCreate, req.Data = ", req.Data)
	// }
	// onEventUserDelete := func(req *protocol.MsEvent) {
	// 	log.Info("run onEventUserDelete, req.Data = ", req.Data)
	// }
	// service.Events["user.create"] = onEventUserCreate
	// service.Events["user.delete"] = onEventUserDelete

	return service
}

//go run .\examples\moleculer-go-demo.go -s nats://192.168.1.69:12008
func main() {

	initFlag()

	printFlag()

	//get NATS server host
	// var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	// flag.Usage = usage
	// flag.Parse()
	// var hosts = strings.Split(*urls, ",")
	// log.Printf("hosts : '%v'\n", hosts)

	//init service and broker
	config := &moleculer.ServiceBrokerConfig{
		NatsHost: gNatsHosts,
		Hostname: "testHostname",
		NodeID:   gNodeID,
		// LogLevel: moleculer.DebugLevel,
		LogLevel: moleculer.ErrorLevel,
		Services: make(map[string]moleculer.Service),
	}
	service := createDemoService()
	config.Services[service.ServiceName] = service
	broker, err := moleculer.NewServiceBroker(config)
	if err != nil {
		log.Fatalf("NewServiceBroker err: %v\n", err)
	}
	pBroker = broker
	broker.Start()

	var callCount uint32
	var minLatency time.Duration
	var maxLatency time.Duration

	test10K := func(wg *sync.WaitGroup) {
		for index := 0; index < gPerThreadCount; index++ {
			startTime := time.Now()
			broker.Call(gMethodName, map[string]interface{}{
				"arg1": "aaa",
				"arg2": 123,
			}, nil)
			endTime := time.Now()
			useTime := endTime.Sub(startTime)
			if minLatency == 0 {
				minLatency = useTime
			}
			if useTime < minLatency {
				minLatency = useTime
			}
			if maxLatency == 0 {
				maxLatency = useTime
			}
			if useTime > maxLatency {
				maxLatency = useTime
			}
			atomic.AddUint32(&callCount, 1)
		}
		wg.Done()
	}

	//test call and emit
	go time.AfterFunc(time.Second*1, func() {
		log.Warnf("broker.Call MethodName[%s] start\n", gMethodName)
		startTime := time.Now()
		wg := sync.WaitGroup{}
		goroutineNum := gThreadNum
		wg.Add(goroutineNum)
		for index := 0; index < goroutineNum; index++ {
			go test10K(&wg)
		}
		wg.Wait()
		endTime := time.Now()
		useTime := endTime.Sub(startTime)
		rps := uint(float64(callCount) / float64(useTime.Nanoseconds()) * 1e9)
		log.Warnf("broker.Call MethodName[%s] end\n", gMethodName)
		// log.Infof("broker.Call demoService.bench %d maxLatency: %s", callCount, maxLatency)
		log.Warnf("broker.Call MethodName[%s] Client[%d] TotalCallCount[%d] use[%s] req/s[%d] minLatency[%s] maxLatency[%s]",
			gMethodName, goroutineNum, callCount, useTime, rps, minLatency, maxLatency)
		// log.Infof("broker.Call demoService.bench %d minLatency: %s", callCount, minLatency)
		// log.Infof("broker.Call demoService.bench %d maxLatency: %s", callCount, maxLatency)
	})

	waitExit()

}
