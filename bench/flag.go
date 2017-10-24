package main

import (
	"flag"
	"strconv"
	"strings"

	nats "github.com/nats-io/go-nats"
)

const (
	//AppName ...
	AppName = "moleculer-go-bench"
	//ServiceName ...
	ServiceName = "benchService"
)

var gUrls string
var gNatsHosts []string
var gMethodName string
var gThreadNum int
var gPerThreadCount int
var gID int
var gNodeID = AppName

func initFlag() {
	_gUrls := flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma, default localhost:4222)")
	_gID := flag.Int("i", 0, "ID of the service on this machine")
	_gMethodName := flag.String("m", "benchService.bench", "The moleculer service method name")
	_gThreadNum := flag.Int("n", 5, "Client number")
	_gPerThreadCount := flag.Int("c", 10000, "each client call method count")

	flag.Usage = usage
	flag.Parse()

	gUrls = *_gUrls
	gID = *_gID
	gMethodName = *_gMethodName
	gThreadNum = *_gThreadNum
	gPerThreadCount = *_gPerThreadCount

	gNatsHosts = strings.Split(gUrls, ",")

	gNodeID += "-" + strconv.Itoa(gID)

}

func printFlag() {
	log.Warnf("gNodeID : %v\n", gNodeID)
	log.Warnf("gUrls : %v\n", gUrls)
	log.Warnf("gNatsHosts : %v\n", gNatsHosts)
	log.Warnf("gMethodName : %v\n", gMethodName)
	log.Warnf("gThreadNum : %v\n", gThreadNum)
	log.Warnf("gPerThreadCount : %v\n", gPerThreadCount)
}
