package moleculer

import (
	"errors"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	logrus "github.com/Sirupsen/logrus"
	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/roytan883/moleculer-go/protocol"

	jsoniter "github.com/json-iterator/go"
)

const (
	//MoleculerLibVersion 0.3.0
	MoleculerLibVersion = "0.3.0"
	//MoleculerProtocolVersion 0.1.0
	MoleculerProtocolVersion = "2"

	defaultRequestTimeout     = time.Second * 5
	natsNodeHeartbeatInterval = time.Second * 5
	natsNodeHeartbeatTimeout  = time.Second * 15
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
	log.WithFields(logrus.Fields{"package": "moleculer-go"})
}

const (
	// PanicLevel level, highest level of severity. Logs and then calls panic with the
	// message passed to Debug, Info, ...
	PanicLevel uint32 = iota
	// FatalLevel level. Logs and then calls `os.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	FatalLevel
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
)

//RequestHandler NOTE that you should not modify req, just return data or error
type RequestHandler func(req *protocol.MsRequest) (interface{}, error)

//EventHandler ...
type EventHandler func(req *protocol.MsEvent)

//Service ...
type Service struct {
	ServiceName string
	Actions     map[string]RequestHandler
	Events      map[string]EventHandler
}

//ServiceBrokerConfig ...
type ServiceBrokerConfig struct {
	NatsHost              []string
	NodeID                string
	LogLevel              uint32        //default 0 (PanicLevel)
	DefaultRequestTimeout time.Duration //default 5s
	Services              map[string]Service
}

//ServiceBroker ...
type ServiceBroker struct {
	config         *ServiceBrokerConfig
	con            *nats.Conn
	selfInfo       *protocol.MsInfoNode
	selfInfoString string
	stoped         chan int
	nodeInfos      sync.Map //map[string]interface{} //original node info
	nodesStatus    sync.Map //map[string(nodeID)]nodeStatusStruct
	actionNodes    sync.Map //map[string(actionFullName)]map[string(nodeID)]string(nodeID)
	eventNodes     sync.Map //map[string(eventFullName)]map[string(nodeID)]string(nodeID)
	waitRequests   sync.Map //map[string(request.ID)]waitResponseStruct
}

type nodeStatusType int

const (
	nodeStatusOnline   nodeStatusType = iota // 0
	nodeStatusHalfOpen                       // 1
	nodeStatusOffline                        // 2
	nodeStatusBusy                           // 3
)

type nodeStatusStruct struct {
	NodeID            string
	LastHeartbeatTime time.Time
	Status            nodeStatusType
}

type waitResponseStruct struct {
	ID        string
	SendTime  time.Time
	Timeout   time.Duration
	Request   *protocol.MsRequest
	Response  *protocol.MsResponse
	WaitChan  chan int
	WaitTimer *time.Timer
}

//NewServiceBroker ...
func NewServiceBroker(config *ServiceBrokerConfig) (*ServiceBroker, error) {

	if stringIsEmptyOrBlank(config.NodeID) {
		log.Error("NewServiceBroker but config.NodeID IsEmptyOrBlank")
		panic("NewServiceBroker but config.NodeID IsEmptyOrBlank")
	}

	serviceBroker := &ServiceBroker{
		con:    nil,
		config: config,
		stoped: make(chan int, 1),
	}

	//TODO: implementation $node.xxx
	// defaultService := Service{
	// 	ServiceName: "$node",
	// 	Actions:     make(map[string]RequestHandler),
	// 	Events:      make(map[string]EventHandler),
	// }
	// defaultService.Actions["$node.list"] = func(req *protocol.MsRequest) (interface{}, error) {
	// 	log.Info("call $node.list")
	// 	return nil, nil
	// }
	// defaultService.Actions["$node.services"] = func(req *protocol.MsRequest) (interface{}, error) {
	// 	log.Info("call $node.services")
	// 	return nil, nil
	// }
	// defaultService.Actions["$node.actions"] = func(req *protocol.MsRequest) (interface{}, error) {
	// 	log.Info("call $node.actions")
	// 	return nil, nil
	// }
	// defaultService.Actions["$node.events"] = func(req *protocol.MsRequest) (interface{}, error) {
	// 	log.Info("call $node.events")
	// 	return nil, nil
	// }
	// defaultService.Actions["$node.health"] = func(req *protocol.MsRequest) (interface{}, error) {
	// 	log.Info("call $node.health")
	// 	return nil, nil
	// }
	// config.Services["$node"] = defaultService

	return serviceBroker, nil
}

//Start ...
func (broker *ServiceBroker) Start() (err error) {

	//at least use two logic cpu avoid block process
	oldCPUs := runtime.GOMAXPROCS(-1)
	if oldCPUs < 2 {
		log.Warn("Set GOMAXPROCS CPU = 2")
		runtime.GOMAXPROCS(2)
	}

	//set LogLevel
	log.SetLevel(logrus.Level(broker.config.LogLevel))

	if broker.con != nil {
		return nil
	}
	natsOpts := nats.DefaultOptions
	//set Name if too long will lead nats-top can't show all messages
	//natsOpts.Name = broker.config.NodeID
	natsOpts.AllowReconnect = true
	natsOpts.ReconnectWait = time.Second * 2 //2s
	natsOpts.MaxReconnect = -1               //If negative, then it will never give up trying to reconnect.
	natsOpts.Servers = broker.config.NatsHost
	con, err := natsOpts.Connect()
	if err != nil {
		log.Error("Nats Can't connect, err = ", err)
		return err
	}
	broker.con = con

	log.Info("Nats Connected")

	var Event = "MOL.EVENT." + broker.config.NodeID
	var Request = "MOL.REQ." + broker.config.NodeID
	var Response = "MOL.RES." + broker.config.NodeID
	var Discover = "MOL.DISCOVER"
	var DiscoverTargetted = "MOL.DISCOVER." + broker.config.NodeID
	var Info = "MOL.INFO"
	var InfoTargetted = "MOL.INFO." + broker.config.NodeID
	var Heartbeat = "MOL.HEARTBEAT"
	var Ping = "MOL.PING"
	var PingTargetted = "MOL.PING." + broker.config.NodeID
	var Pong = "MOL.PONG." + broker.config.NodeID
	var Disconnect = "MOL.DISCONNECT"

	_, err = con.Subscribe(Event, broker._onEvent)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Event)
		return err
	}

	_, err = con.Subscribe(Request, broker._onRequest)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Request)
		return err
	}

	// for _, service := range broker.config.Services {
	// 	if stringIsNotEmpty(service.ServiceName) {
	// 		var topic = "MOL.REQB." + service.ServiceName + ".>"
	// 		_, err = con.Subscribe(topic, broker._onRequest)
	// 		if err != nil {
	// 			log.Error("Nats Can't Subscribe: ", Request)
	// 			return err
	// 		}
	// 	}
	// }

	_, err = con.Subscribe(Response, broker._onResponse)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Response)
		return err
	}

	_, err = con.Subscribe(Discover, broker._onDiscover)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Discover)
		return err
	}

	_, err = con.Subscribe(DiscoverTargetted, broker._onDiscoverTargetted)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", DiscoverTargetted)
		return err
	}

	_, err = con.Subscribe(Info, broker._onInfo)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Info)
		return err
	}

	_, err = con.Subscribe(InfoTargetted, broker._onInfoTargetted)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", InfoTargetted)
		return err
	}

	_, err = con.Subscribe(Heartbeat, broker._onHeartbeat)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Heartbeat)
		return err
	}

	_, err = con.Subscribe(Ping, broker._onPing)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Ping)
		return err
	}

	_, err = con.Subscribe(PingTargetted, broker._onPingTargetted)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", PingTargetted)
		return err
	}

	_, err = con.Subscribe(Pong, broker._onPong)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Pong)
		return err
	}

	_, err = con.Subscribe(Disconnect, broker._onDisconnect)
	if err != nil {
		log.Error("Nats Can't Subscribe: ", Disconnect)
		return err
	}

	broker._genselfInfo()
	broker._broadcastDiscover()
	broker._startHeartbeatTimer()

	return nil
}

//Stop ...
func (broker *ServiceBroker) Stop() (err error) {
	if broker.con == nil {
		return
	}
	broker.con.Close()
	broker.con = nil
	//TODO: clean

	broker.stoped <- 1
	return nil
}

func (broker *ServiceBroker) _startHeartbeatTimer() {
	go func() {
		for {
			select {
			case <-broker.stoped:
				log.Warn("_startHeartbeatTimer stopped")
				return

			case <-time.After(natsNodeHeartbeatInterval):
				broker._broadcastHeartbeat()
				broker._checkNodesTimeout()
			}
		}
	}()
}

func (broker *ServiceBroker) _checkNodesTimeout() {
	nowTime := time.Now()
	broker.nodesStatus.Range(func(key, value interface{}) bool {
		nodeStatusObj, ok := value.(nodeStatusStruct)
		if ok {
			oldTime := nodeStatusObj.LastHeartbeatTime
			if nowTime.Sub(oldTime) >= natsNodeHeartbeatTimeout {
				broker.nodesStatus.Delete(key)
				// if nodeStatusObj.Status != nodeStatusOffline {
				// 	log.Warn("node timeout, set nodeStatusOffline: ", key)
				// 	nodeStatusObj.Status = nodeStatusOffline
				// 	broker.nodesStatus.Store(key, nodeStatusObj)
				// }
			}
		}
		return true
	})
}

//CallOptions ...
type CallOptions struct {
	Timeout    time.Duration
	RetryCount uint64 //not support now
	NodeID     string
	Meta       interface{}
}

//Call you can set opts=nil, or custom opts.Timeout (default:5s)
func (broker *ServiceBroker) Call(action string, params interface{}, opts *CallOptions) (data interface{}, err error) {
	log.Info("Call: ", action)
	var _opts = opts
	if _opts == nil {
		_opts = &CallOptions{
			Timeout: broker.config.DefaultRequestTimeout,
		}
	}
	if _opts.Timeout <= 0 {
		_opts.Timeout = defaultRequestTimeout
	}

	_referNodes, ok := broker.actionNodes.Load(action)
	if !ok {
		log.Warn("Call can't find node for: ", action)
		return nil, errors.New("Service Not available: " + action)
	}
	referNodes, ok := _referNodes.(map[string]string)
	if !ok {
		log.Warn("Call can't find node err interface to map[string]string")
		return nil, errors.New("Service Not available: " + action)
	}
	log.Info("Call referNodes = ", referNodes)
	var chooseNodeID = ""
	if stringIsNotEmpty(_opts.NodeID) {
		_, ok := referNodes[_opts.NodeID]
		if !ok {
			log.Warn("Call can't find dst node: ", _opts.NodeID)
			return nil, errors.New("Service Not available: " + action)
		}
		chooseNodeID = _opts.NodeID
	}
	nodesCount := len(referNodes)
	if nodesCount < 1 {
		log.Warn("Call can't find node in actionNodes map")
		return nil, errors.New("Service Not available: " + action)
	}
	onlineNodes := make([]string, 0)
	for nodeID := range referNodes {
		nodeStatusData, ok := broker.nodesStatus.Load(nodeID)
		if ok {
			nodeStatusObj, ok2 := nodeStatusData.(nodeStatusStruct)
			if ok2 && nodeStatusObj.Status == nodeStatusOnline {
				onlineNodes = append(onlineNodes, nodeStatusObj.NodeID)
			}
		}
	}
	onlineNodesCount := len(onlineNodes)
	if onlineNodesCount < 1 {
		log.Warn("Call can't find node in nodesStatus map with online")
		return nil, errors.New("Service Not available: " + action)
	}

	chooseIndex := rand.Intn(onlineNodesCount)
	chooseNodeID = onlineNodes[chooseIndex]

	requestObj := &protocol.MsRequest{
		Ver:       MoleculerProtocolVersion,
		Sender:    broker.config.NodeID,
		ID:        nuid.New().Next(),
		Action:    action,
		Params:    params,
		Meta:      _opts.Meta,
		Timeout:   float64(_opts.Timeout.Seconds() * 1000),
		Level:     1,
		Metrics:   false,
		ParentID:  "",
		RequestID: "",
	}
	sendData, err := jsoniter.Marshal(requestObj)
	if err != nil {
		log.Error("Call Marshal sendData error: ", err)
		return nil, errors.New("JSON Marshal Data Error: " + action)
	}
	log.Info("Call topic: ", "MOL.REQ."+chooseNodeID)
	log.Info("Call data: ", string(sendData))

	waitResponseObj := &waitResponseStruct{
		ID:        requestObj.ID,
		SendTime:  time.Now(),
		Timeout:   _opts.Timeout,
		Request:   requestObj,
		WaitChan:  make(chan int, 1),
		WaitTimer: time.NewTimer(_opts.Timeout),
	}
	broker.waitRequests.Store(waitResponseObj.ID, waitResponseObj)

	broker.con.Publish("MOL.REQ."+chooseNodeID, sendData)

	select {
	case <-waitResponseObj.WaitChan:
		log.Info("Call response: ", waitResponseObj.Response)
		waitResponseObj.WaitTimer.Stop()
		broker.waitRequests.Delete(waitResponseObj.ID)
		return waitResponseObj.Response.Data, nil

	case <-waitResponseObj.WaitTimer.C:
		broker.waitRequests.Delete(waitResponseObj.ID)
		return nil, errors.New("Request timeout: " + action)
	}

}

//Emit ...
func (broker *ServiceBroker) Emit(event string, params interface{}) (err error) {
	log.Info("Emit: ", event)

	_referNodes, ok := broker.eventNodes.Load(event)
	if !ok {
		log.Warn("Emit can't find node for: ", event)
		return errors.New("Service Not available: " + event)
	}
	referNodes, ok := _referNodes.(map[string]string)
	if !ok {
		log.Warn("Emit can't find node err interface to map[string]string")
		return errors.New("Service Not available: " + event)
	}
	log.Info("Emit referNodes = ", referNodes)
	var chooseNodeID = ""
	nodesCount := len(referNodes)
	if nodesCount < 1 {
		log.Warn("Emit can't find node in eventNodes map")
		return errors.New("Service Not available: " + event)
	}
	onlineNodes := make([]string, 0)
	for nodeID := range referNodes {
		nodeStatusData, ok := broker.nodesStatus.Load(nodeID)
		if ok {
			nodeStatusObj, ok2 := nodeStatusData.(nodeStatusStruct)
			if ok2 && nodeStatusObj.Status == nodeStatusOnline {
				onlineNodes = append(onlineNodes, nodeStatusObj.NodeID)
			}
		}
	}
	onlineNodesCount := len(onlineNodes)
	if onlineNodesCount < 1 {
		log.Warn("Emit can't find node in nodesStatus map with online")
		return errors.New("Service Not available: " + event)
	}

	chooseIndex := rand.Intn(onlineNodesCount)
	chooseNodeID = onlineNodes[chooseIndex]

	requestObj := &protocol.MsEvent{
		Ver:    MoleculerProtocolVersion,
		Sender: broker.config.NodeID,
		Event:  event,
		Data:   params,
	}
	sendData, err := jsoniter.Marshal(requestObj)
	if err != nil {
		log.Error("Emit Marshal sendData error: ", err)
		return errors.New("JSON Marshal Data Error: " + event)
	}
	log.Info("Emit topic: ", "MOL.EVENT."+chooseNodeID)
	log.Info("Emit data: ", string(sendData))

	broker.con.Publish("MOL.EVENT."+chooseNodeID, sendData)

	return nil
}

//Broadcast ...
func (broker *ServiceBroker) Broadcast(event string, params interface{}) (err error) {
	log.Info("Broadcast: ", event)

	_referNodes, ok := broker.eventNodes.Load(event)
	if !ok {
		log.Warn("Broadcast can't find node for: ", event)
		return errors.New("Service Not available: " + event)
	}
	referNodes, ok := _referNodes.(map[string]string)
	if !ok {
		log.Warn("Broadcast can't find node err interface to map[string]string")
		return errors.New("Service Not available: " + event)
	}
	log.Info("Broadcast referNodes = ", referNodes)
	nodesCount := len(referNodes)
	if nodesCount < 1 {
		log.Warn("Broadcast can't find node in eventNodes map")
		return errors.New("Service Not available: " + event)
	}
	onlineNodes := make([]string, 0)
	for nodeID := range referNodes {
		nodeStatusData, ok := broker.nodesStatus.Load(nodeID)
		if ok {
			nodeStatusObj, ok2 := nodeStatusData.(nodeStatusStruct)
			if ok2 && nodeStatusObj.Status == nodeStatusOnline {
				onlineNodes = append(onlineNodes, nodeStatusObj.NodeID)
			}
		}
	}

	requestObj := &protocol.MsEvent{
		Ver:    MoleculerProtocolVersion,
		Sender: broker.config.NodeID,
		Event:  event,
		Data:   params,
	}
	sendData, err := jsoniter.Marshal(requestObj)
	if err != nil {
		log.Error("Broadcast Marshal sendData error: ", err)
		return errors.New("JSON Marshal Data Error: " + event)
	}

	for _, nodeID := range onlineNodes {
		log.Info("Broadcast topic: ", "MOL.EVENT."+nodeID)
		log.Info("Broadcast data: ", string(sendData))
		broker.con.Publish("MOL.EVENT."+nodeID, sendData)
	}

	return nil
}

func (broker *ServiceBroker) _broadcastDiscover() {
	sendData, err := jsoniter.Marshal(&protocol.MsDiscover{
		Ver:    MoleculerProtocolVersion,
		Sender: broker.config.NodeID,
	})
	if err != nil {
		log.Error("NATS _broadcastDiscover Marshal err: ", err)
		panic(err)
	}
	broker.con.Publish("MOL.DISCOVER", sendData)
	return
}

func (broker *ServiceBroker) _broadcastInfo() {
	var infoString = broker._genselfInfo()
	broker.con.Publish("MOL.INFO", []byte(infoString))
	return
}

func (broker *ServiceBroker) _broadcastHeartbeat() {
	// log.Info("_broadcastHeartbeat")
	sendData, err := jsoniter.Marshal(&protocol.MsHeartbeat{
		Ver:    MoleculerProtocolVersion,
		Sender: broker.config.NodeID,
		CPU:    0,
	})
	if err != nil {
		log.Error("NATS _broadcastHeartbeat Marshal err: ", err)
		panic(err)
	}
	broker.con.Publish("MOL.HEARTBEAT", sendData)
	return
}

func (broker *ServiceBroker) _onEvent(msg *nats.Msg) {
	go func() {
		log.Info("NATS _onEvent: ", string(msg.Data))
		jsonObj := &protocol.MsEvent{}
		err := jsoniter.Unmarshal(msg.Data, jsonObj)
		if err != nil {
			log.Error("NATS _onEvent parse err: ", err)
			return
		}
		eventName := jsonObj.Event
		for _, service := range broker.config.Services {
			eventHandler, ok := service.Events[eventName]
			if ok {
				go eventHandler(jsonObj)
			}
		}
	}()
}

func _splitNames(str string) (string, string, error) {
	names := strings.Split(str, ".")
	if len(names) != 2 {
		log.Error("NATS _splitNames find names err: ", str)
		return "", "", errors.New("splitNames names from string: " + str)
	}
	var name1 = names[0]
	var name2 = names[1]
	return name1, name2, nil
}

func (broker *ServiceBroker) _onRequest(msg *nats.Msg) {
	go func() {
		log.Info("NATS _onRequest: ", string(msg.Data))
		jsonObj := &protocol.MsRequest{}
		err := jsoniter.Unmarshal(msg.Data, jsonObj)
		if err != nil {
			log.Error("NATS _onRequest parse err: ", err)
			return
		}
		serviceName, actionName, errParse := _splitNames(jsonObj.Action)
		if errParse != nil {
			log.Error("NATS _onRequest find action err: ", errParse)
			return
		}
		service, ok := broker.config.Services[serviceName]
		if !ok {
			log.Error("NATS _onRequest find service err: ", serviceName)
			return
		}
		action, ok2 := service.Actions[actionName]
		if !ok2 {
			log.Error("NATS _onRequest find action err: ", actionName)
			return
		}
		jsonRes := &protocol.MsResponse{}
		jsonResData, err := action(jsonObj)
		if err != nil {
			jsonRes.Success = false
			jsonRes.Error = map[string]interface{}{
				"name":    err.Error(),
				"message": err.Error(),
				"nodeID":  broker.config.NodeID,
			}
		} else {
			jsonRes.Success = true
			jsonRes.Data = jsonResData
		}
		jsonRes.Ver = jsonObj.Ver
		jsonRes.Sender = broker.config.NodeID
		jsonRes.ID = jsonObj.ID
		// jsonRes.Success = true //should set by app layer
		sendData, err3 := jsoniter.Marshal(jsonRes)
		if err3 != nil {
			log.Error("NATS _onRequest Marshal jsonRes err: ", err3)
			return
		}
		log.Info("NATS _onRequest replay to : ", "MOL.RES."+jsonObj.Sender)
		log.Info("NATS _onRequest sendData : ", string(sendData))
		broker.con.Publish("MOL.RES."+jsonObj.Sender, sendData)

		return

	}()
}
func (broker *ServiceBroker) _onResponse(msg *nats.Msg) {
	go func() {

		log.Info("NATS _onResponse: ", string(msg.Data))
		jsonObj := &protocol.MsResponse{}
		err := jsoniter.Unmarshal(msg.Data, jsonObj)
		if err != nil {
			log.Error("NATS _onResponse parse err: ", err)
			return
		}
		waitResponseObjData, ok := broker.waitRequests.Load(jsonObj.ID)
		if !ok {
			log.Error("NATS _onResponse can't find request ID : ", jsonObj.ID)
			return
		}
		waitResponseObj, ok := waitResponseObjData.(*waitResponseStruct)
		if !ok {
			log.Warn("Call can't find node err interface to waitResponseStruct")
			return
		}
		waitResponseObj.Response = jsonObj
		waitResponseObj.WaitChan <- 1

		return

	}()
}
func (broker *ServiceBroker) _onDiscover(msg *nats.Msg) {
	log.Info("NATS _onDiscover: ", string(msg.Data))
	broker._broadcastInfo()
	return
}
func (broker *ServiceBroker) _onDiscoverTargetted(msg *nats.Msg) {
	log.Info("NATS _onDiscoverTargetted: ", string(msg.Data))
	jsonObj := &protocol.MsDiscover{}
	err := jsoniter.Unmarshal(msg.Data, jsonObj)
	if err != nil {
		log.Error("NATS _onDiscoverTargetted err: ", err)
		return
	}
	if stringIsNotEmpty(jsonObj.Sender) {
		log.Info("NATS _onDiscoverTargetted send selfInfo to : ", "MOL.INFO."+jsonObj.Sender)
		infoString := broker._genselfInfo()
		broker.con.Publish("MOL.INFO."+jsonObj.Sender, []byte(infoString))
	}
	return
}
func (broker *ServiceBroker) _onInfo(msg *nats.Msg) {
	s := string(msg.Data)
	log.Info("NATS _onInfo: ", s)
	info := &protocol.MsInfoNode{}
	err := jsoniter.Unmarshal(msg.Data, info)
	if err != nil {
		log.Error("parse MsInfoNode error:", err)
		return
	}
	broker._handlerNodeInfo(info)
	return
}
func (broker *ServiceBroker) _onInfoTargetted(msg *nats.Msg) {
	s := string(msg.Data)
	log.Info("NATS _onInfoTargetted: ", s)
	info := &protocol.MsInfoNode{}
	err := jsoniter.Unmarshal(msg.Data, info)
	if err != nil {
		log.Error("parse MsInfoNode error:", err)
		return
	}
	broker._handlerNodeInfo(info)
	return
}
func (broker *ServiceBroker) _onHeartbeat(msg *nats.Msg) {
	// log.Info("NATS _onHeartbeat: ", string(msg.Data))
	jsonObj := &protocol.MsHeartbeat{}
	err := jsoniter.Unmarshal(msg.Data, jsonObj)
	if err != nil {
		log.Error("parse MsHeartbeat error:", err)
		return
	}
	nodeStatusObj := nodeStatusStruct{
		NodeID:            jsonObj.Sender,
		LastHeartbeatTime: time.Now(),
		Status:            nodeStatusOnline,
	}
	broker.nodesStatus.Store(jsonObj.Sender, nodeStatusObj)
	return
}
func (broker *ServiceBroker) _onPing(msg *nats.Msg) {
	log.Info("NATS _onPing: ", string(msg.Data))
	jsonObj := &protocol.MsPing{}
	err := jsoniter.Unmarshal(msg.Data, jsonObj)
	if err != nil {
		log.Error("NATS _onPing err: ", err)
		return
	}
	if stringIsNotEmpty(jsonObj.Sender) {
		sendData, err := jsoniter.Marshal(&protocol.MsPong{
			Ver:     MoleculerProtocolVersion,
			Sender:  broker.config.NodeID,
			Time:    jsonObj.Time,
			Arrived: getNowMilliseconds(),
		})
		if err != nil {
			log.Error("NATS _onPing Marshal err: ", err)
			return
		}
		broker.con.Publish("MOL.PONG."+jsonObj.Sender, sendData)
	}
	return
}
func (broker *ServiceBroker) _onPingTargetted(msg *nats.Msg) {
	log.Info("NATS _onPingTargetted: ", string(msg.Data))
	return
}
func (broker *ServiceBroker) _onPong(msg *nats.Msg) {
	log.Info("NATS _onPong: ", string(msg.Data))
	return
}
func (broker *ServiceBroker) _onDisconnect(msg *nats.Msg) {
	log.Info("NATS _onDisconnect: ", string(msg.Data))
	jsonObj := &protocol.MsDiscover{}
	err := jsoniter.Unmarshal(msg.Data, jsonObj)
	if err != nil {
		log.Error("NATS _onDisconnect err: ", err)
		return
	}

	broker.nodesStatus.Delete(jsonObj.Sender)

	return
}

func (broker *ServiceBroker) _handlerNodeInfo(info *protocol.MsInfoNode) {
	broker.nodeInfos.Store(info.Sender, info)

	nodeID := info.Sender

	for _, service := range info.Services {
		//parse and save other node's actions
		for _, action := range service.Actions {
			actionName := action.Name

			nodes := map[string]string{}
			nodes[nodeID] = nodeID
			oldNodes, loaded := broker.actionNodes.LoadOrStore(actionName, nodes)
			if loaded {
				_oldNodes, ok := oldNodes.(map[string]string)
				if ok {
					_oldNodes[nodeID] = nodeID
					broker.actionNodes.Store(actionName, oldNodes)
				}
			}
		}
		//parse and save other node's events
		for _, event := range service.Events {
			eventName := event.Name

			nodes := map[string]string{}
			nodes[nodeID] = nodeID
			oldNodes, loaded := broker.eventNodes.LoadOrStore(eventName, nodes)
			if loaded {
				_oldNodes, ok := oldNodes.(map[string]string)
				if ok {
					_oldNodes[nodeID] = nodeID
					broker.eventNodes.Store(eventName, oldNodes)
				}
			}
		}
	}

	nodeStatusObj := nodeStatusStruct{
		NodeID:            nodeID,
		LastHeartbeatTime: time.Now(),
		Status:            nodeStatusOnline,
	}
	log.Info("_handlerNodeInfo nodeID = ", nodeID)
	log.Info("_handlerNodeInfo nodeStatusObj = ", nodeStatusObj)
	broker.nodesStatus.Store(nodeID, nodeStatusObj)
}

func (broker *ServiceBroker) _genselfInfo() string {
	if stringIsNotEmpty(broker.selfInfoString) {
		return broker.selfInfoString
	}
	log.Info("_genselfInfo: ")
	var nodeInfo = &protocol.MsInfoNode{}
	broker.selfInfo = nodeInfo
	nodeInfo.Ver = MoleculerProtocolVersion
	nodeInfo.Client.Type = "go"
	nodeInfo.Client.Version = MoleculerLibVersion
	nodeInfo.Client.LangVersion = "1.9.0"
	nodeInfo.Sender = broker.config.NodeID
	nodeInfo.IPList = getLanIPs()
	nodeInfo.Services = make([]protocol.MsInfoService, 0, 5)

	for _, service := range broker.config.Services {
		var serviceInfo = &protocol.MsInfoService{}
		serviceInfo.Name = service.ServiceName
		serviceInfo.NodeID = broker.config.NodeID

		serviceInfo.Actions = make(map[string]protocol.MsInfoAction)
		for actionName := range service.Actions {
			combineActionName := service.ServiceName + "." + actionName
			serviceInfo.Actions[combineActionName] = protocol.MsInfoAction{
				Name:  combineActionName,
				Cache: false,
			}
		}

		serviceInfo.Events = make(map[string]protocol.MsInfoEvent)
		for eventName := range service.Events {
			serviceInfo.Events[eventName] = protocol.MsInfoEvent{
				Name: eventName,
			}
		}

		nodeInfo.Services = append(nodeInfo.Services, *serviceInfo)
	}

	jsonBytes, err := jsoniter.Marshal(nodeInfo)
	if err != nil {
		log.Error("_genselfInfo Marshal err: ", err)
		panic(err)
	}
	broker.selfInfoString = string(jsonBytes)

	log.Info("_genselfInfo = ", broker.selfInfoString)
	return broker.selfInfoString
}

// StringIsEmpty returns true if the string is empty
func stringIsEmpty(text string) bool {
	return len(text) == 0
}

// StringIsNotEmpty returns true if the string is not empty
func stringIsNotEmpty(text string) bool {
	return !stringIsEmpty(text)
}

// StringIsBlank returns true if the string is blank (all whitespace)
func stringIsBlank(text string) bool {
	return len(strings.TrimSpace(text)) == 0
}

// StringIsNotBlank returns true if the string is not blank
func stringIsNotBlank(text string) bool {
	return !stringIsBlank(text)
}

// stringIsEmptyOrBlank returns true if the string is empty or blank (all whitespace)
func stringIsEmptyOrBlank(text string) bool {
	return stringIsEmpty(text) && stringIsBlank(text)
}

// stringIsNotEmptyOrBlank returns true if the string is not empty or blank (all whitespace)
func stringIsNotEmptyOrBlank(text string) bool {
	return !stringIsEmptyOrBlank(text)
}

func getNowMilliseconds() int64 {
	return time.Now().UnixNano() / 1e6
}

func getLanIP() string {
	ips := make([]string, 0)
	//ips := make([]string, 10)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("net.InterfaceAddrs err:" + err.Error())
		os.Exit(1)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				//os.Stdout.WriteString(ipnet.IP.String() + "\n")
				ips = append(ips, ipnet.IP.String())
			}
		}
	}
	ret := ""
	for _, value := range ips {
		if !strings.HasSuffix(value, ".1") && !strings.HasSuffix(value, ".0") {
			ret = value
			return ret
		}
	}
	return ret
}

func getLanIPs() []string {
	ips := make([]string, 0)

	//ips := make([]string, 10)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("net.InterfaceAddrs err:" + err.Error())
		os.Exit(1)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				//os.Stdout.WriteString(ipnet.IP.String() + "\n")
				ips = append(ips, ipnet.IP.String())
			}
		}
	}

	retIPs := make([]string, 0)
	for _, value := range ips {
		if !strings.HasSuffix(value, ".1") && !strings.HasSuffix(value, ".0") {
			retIPs = append(retIPs, value)
		}
	}
	return retIPs
}
