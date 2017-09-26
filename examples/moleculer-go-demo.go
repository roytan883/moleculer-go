package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"

	nats "github.com/nats-io/go-nats"
	moleculer "github.com/roytan883/moleculer"
	"github.com/roytan883/moleculer/protocol"
)

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

	log.Warn("=================== exit rt-go-api-framework =================== ")
	os.Exit(0)
}

// NOTE: Use tls scheme for TLS, e.g. nats-req -s tls://demo.nats.io:4443 foo hello
func usage() {
	log.Fatalf("Usage: moleculer-go-demo [-s server (%s)] \n", nats.DefaultURL)
}

func fnAAA(req *protocol.MsRequest) *protocol.MsResponse {
	log.Info("call AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	res := &protocol.MsResponse{}
	data := map[string]interface{}{
		"a": 111,
		"b": "abc",
		"c": true,
	}
	res.Data = data
	return res
}

func onEventBBB(req *protocol.MsRequest) *protocol.MsResponse {
	log.Info("call onEventBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	return &protocol.MsResponse{}
}

func main() {
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

	// func fn1(params interface{}) (int, error) {
	// 	return 111, nil
	// }

	service := moleculer.Service{
		ServiceName: "demo",
		Actions:     make(map[string]moleculer.CallbackFunc),
		Events:      make(map[string]moleculer.CallbackFunc),
	}
	service.Actions["fnAAA"] = fnAAA
	service.Events["demo.eventB"] = onEventBBB

	config := &moleculer.ServiceBrokerConfig{
		NatsHost: hosts,
		NodeID:   "moleculer-go-demo",
		Services: make(map[string]moleculer.Service),
	}
	config.Services["demo"] = service

	broker, err := moleculer.NewServiceBroker(config)
	if err != nil {
		log.Fatalf("NewServiceBroker err: %v\n", err)
	}
	broker.Start()

	waitExit()

	// nc, err := nats.Connect(*urls)
	// if err != nil {
	// 	log.Fatalf("Can't connect: %v\n", err)
	// }
	// defer nc.Close()
	// subj, payload := args[0], []byte(args[1])

	// msg, err := nc.Request(subj, []byte(payload), 100*time.Millisecond)
	// if err != nil {
	// 	if nc.LastError() != nil {
	// 		log.Fatalf("Error in Request: %v\n", nc.LastError())
	// 	}
	// 	log.Fatalf("Error in Request: %v\n", err)
	// }

	// log.Printf("Published [%s] : '%s'\n", subj, payload)
	// log.Printf("Received [%v] : '%s'\n", msg.Subject, string(msg.Data))
}
