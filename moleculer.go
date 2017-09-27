package moleculer

import (
	"errors"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	logrus "github.com/Sirupsen/logrus"
	nats "github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/roytan883/moleculer/protocol"

	jsoniter "github.com/json-iterator/go"
	stringsUtils "github.com/shomali11/util/strings"
)

//CallbackFunc ...
type CallbackFunc func(req *protocol.MsRequest) *protocol.MsResponse

//Service ...
type Service struct {
	ServiceName string
	Actions     map[string]CallbackFunc
	Events      map[string]CallbackFunc
}

//ServiceBrokerConfig ...
type ServiceBrokerConfig struct {
	NatsHost []string
	NodeID   string
	LogLevel logrus.Level
	Services map[string]Service
}

//ServiceBroker ...
type ServiceBroker struct {
	config         *ServiceBrokerConfig
	con            *nats.Conn
	selfInfo       *protocol.MsInfoNode
	selfInfoString string
	stoped         chan int
	nodeInfos      sync.Map //map[string]interface{} //original node info
	nodesStatus    sync.Map //map[string]nodeStatus
	actionNodes    sync.Map //map[string(actionFullName)]map[string(nodeID)]string(nodeID)
	waitRequests   sync.Map //map[string(id)]waitResponse
}

type nodeStatusType int

const (
	nodeStatusOnline   nodeStatusType = iota // 0
	nodeStatusHalfOpen                       // 1
	nodeStatusOffline                        // 2
	nodeStatusBusy                           // 3
)

type nodeStatus struct {
	NodeID            string
	LastHeartbeatTime time.Time
	Status            nodeStatusType
}

type waitResponse struct {
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
	serviceBroker := &ServiceBroker{
		con:    nil,
		config: config,
		stoped: make(chan int, 1),
	}

	defaultService := Service{
		ServiceName: "$node",
		Actions:     make(map[string]CallbackFunc),
		Events:      make(map[string]CallbackFunc),
	}
	defaultService.Actions["$node.list"] = func(req *protocol.MsRequest) *protocol.MsResponse {
		log.Info("call $node.list")
		return &protocol.MsResponse{}
	}
	defaultService.Actions["$node.services"] = func(req *protocol.MsRequest) *protocol.MsResponse {
		log.Info("call $node.services")
		return &protocol.MsResponse{}
	}
	defaultService.Actions["$node.actions"] = func(req *protocol.MsRequest) *protocol.MsResponse {
		log.Info("call $node.actions")
		return &protocol.MsResponse{}
	}
	defaultService.Actions["$node.events"] = func(req *protocol.MsRequest) *protocol.MsResponse {
		log.Info("call $node.events")
		return &protocol.MsResponse{}
	}
	defaultService.Actions["$node.health"] = func(req *protocol.MsRequest) *protocol.MsResponse {
		log.Info("call $node.health")
		return &protocol.MsResponse{}
	}
	config.Services["$node"] = defaultService

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
	log.SetLevel(broker.config.LogLevel)

	// jsoniter.Marshal(&data)

	// value := gjson.Get(json, "name.last")
	// gjson.Parse(json)
	// println(value.String())

	if broker.con != nil {
		return
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
	// 	if strings.IsNotEmpty(service.ServiceName) {
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
	// log.Warn("_genselfInfo = ", )

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

			case <-time.After(time.Second * 3):
				broker._broadcastHeartbeat()
				broker._checkNodesTimeout()
				//CHECK nodesStatus

				// case <-time.After(time.Second * 15):
				// 	println("10s timer")
			}
		}
	}()
}

func (broker *ServiceBroker) _checkNodesTimeout() {
	nowTime := time.Now()
	broker.nodesStatus.Range(func(key, value interface{}) bool {
		nodeStatusObj, ok := value.(nodeStatus)
		if ok {
			oldTime := nodeStatusObj.LastHeartbeatTime
			if nowTime.Sub(oldTime) >= time.Second*6 {
				// broker.nodesStatus.Delete(key)
				if nodeStatusObj.Status != nodeStatusOffline {
					log.Warn("node timeout, set nodeStatusOffline: ", key)
					nodeStatusObj.Status = nodeStatusOffline
					broker.nodesStatus.Store(key, nodeStatusObj)
				}
			}
		}
		return true
	})
}

//CallOptions ...
type CallOptions struct {
	Timeout    time.Duration
	RetryCount uint64
	NodeID     string
	Meta       interface{}
}

//Call defaultTimeout 5s
// Call("demo.fn1", map[string]interface{}{
// 	"a": 111,
// 	"b": "abc",
// 	"c": true,
// }, nil)
func (broker *ServiceBroker) Call(action string, params interface{}, opts *CallOptions) (data interface{}, err error) {
	log.Info("Call: ", action)
	var _opts = opts
	if _opts == nil {
		_opts = &CallOptions{
			Timeout:    time.Second * 5,
			RetryCount: 0,
			NodeID:     "",
			Meta:       nil,
		}
	}

	oldNodes, ok := broker.actionNodes.Load(action)
	if !ok {
		log.Warn("Call can't find node for: ", action)
		return nil, errors.New("Service Not available: " + action)
	}
	_oldNodes, ok := oldNodes.(map[string]string)
	if !ok {
		log.Warn("Call can't find node err interface to map[string]string")
		return nil, errors.New("Service Not available: " + action)
	}
	log.Info("Call _oldNodes = ", _oldNodes)
	var chooseNodeID = ""
	if stringsUtils.IsNotEmpty(_opts.NodeID) {
		_, ok := _oldNodes[_opts.NodeID]
		if !ok {
			log.Warn("Call can't find dst node: ", _opts.NodeID)
			return nil, errors.New("Service Not available: " + action)
		}
		chooseNodeID = _opts.NodeID
	}
	nodesCount := len(_oldNodes)
	if nodesCount < 1 {
		log.Warn("Call can't find node in actionNodes map")
		return nil, errors.New("Service Not available: " + action)
	}
	onlineNodes := make([]string, 0)
	for nodeID := range _oldNodes {
		nodeStatusData, ok := broker.nodesStatus.Load(nodeID)
		if ok {
			nodeStatusObj, ok2 := nodeStatusData.(nodeStatus)
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
		Ver:       "2",
		Sender:    broker.config.NodeID,
		ID:        nuid.New().Next(),
		Action:    action,
		Params:    params,
		Meta:      _opts.Meta,
		Timeout:   uint32(_opts.Timeout.Seconds() * 1000),
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

	waitResponseObj := &waitResponse{
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
func (broker *ServiceBroker) Emit(event string, data []byte) (err error) {
	return nil
}

//Broadcast ...
func (broker *ServiceBroker) Broadcast(event string, data []byte) (err error) {
	return nil
}

func (broker *ServiceBroker) _broadcastDiscover() {
	sendData, err := jsoniter.Marshal(&protocol.MsDiscover{
		Ver:    "2",
		Sender: broker.config.NodeID,
	})
	if err != nil {
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
		Ver:    "2",
		Sender: broker.config.NodeID,
		CPU:    0,
	})
	if err != nil {
		panic(err)
	}
	broker.con.Publish("MOL.HEARTBEAT", sendData)
	return
}

func (broker *ServiceBroker) _onEvent(msg *nats.Msg) {
	log.Info("NATS _onEvent: ", string(msg.Data))
	return
}

func _getServiceAndAction(str string) (string, string, error) {
	actionNames := strings.Split(str, ".")
	if len(actionNames) != 2 {
		log.Error("NATS _onRequest find action err: ", actionNames)
		return "", "", errors.New("get ServiceAndAction name error from string: " + str)
	}
	var serviceName = actionNames[0]
	var actionName = actionNames[1]
	return serviceName, actionName, nil
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
		serviceName, actionName, errParse := _getServiceAndAction(jsonObj.Action)
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
		jsonRes := action(jsonObj)
		jsonRes.Ver = jsonObj.Ver
		jsonRes.Sender = broker.config.NodeID
		jsonRes.ID = jsonObj.ID
		jsonRes.Success = true
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
	// go func() {

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
	waitResponseObj, ok := waitResponseObjData.(*waitResponse)
	if !ok {
		log.Warn("Call can't find node err interface to waitResponse")
		return
	}
	waitResponseObj.Response = jsonObj
	waitResponseObj.WaitChan <- 1

	return

	// }()
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
	}
	if stringsUtils.IsNotEmpty(jsonObj.Sender) {
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
	nodeStatusObj := nodeStatus{
		NodeID:            jsonObj.Sender,
		LastHeartbeatTime: time.Now(),
		Status:            nodeStatusOnline,
	}
	broker.nodesStatus.Store(jsonObj.Sender, nodeStatusObj)
	return
}
func (broker *ServiceBroker) _onPing(msg *nats.Msg) {
	log.Info("NATS _onPing: ", string(msg.Data))
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
	return
}

func (broker *ServiceBroker) _handlerNodeInfo(info *protocol.MsInfoNode) {
	broker.nodeInfos.Store(info.Sender, info)

	nodeID := info.Sender

	for _, service := range info.Services {
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
	}

	nodeStatusObj := nodeStatus{
		NodeID:            nodeID,
		LastHeartbeatTime: time.Now(),
		Status:            nodeStatusOnline,
	}
	log.Info("_handlerNodeInfo nodeID = ", nodeID)
	log.Info("_handlerNodeInfo nodeStatusObj = ", nodeStatusObj)
	broker.nodesStatus.Store(nodeID, nodeStatusObj)
}

func (broker *ServiceBroker) _genselfInfo() string {
	if stringsUtils.IsNotEmpty(broker.selfInfoString) {
		return broker.selfInfoString
	}
	log.Info("_genselfInfo: ")
	var nodeInfo = &protocol.MsInfoNode{}
	broker.selfInfo = nodeInfo
	nodeInfo.Ver = "2"
	nodeInfo.Client.Type = "go"
	nodeInfo.Client.Version = "0.1.0"
	nodeInfo.Client.LangVersion = "1.9.0"
	nodeInfo.Sender = broker.config.NodeID
	nodeInfo.IPList = make([]string, 0, 5)
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
		panic(err)
	}
	broker.selfInfoString = string(jsonBytes)

	return broker.selfInfoString
}

func init() {
	initLog()
}

var log *logrus.Logger

func initLog() {
	log = logrus.New()
	log.Formatter = &logrus.TextFormatter{
		FullTimestamp: true,
		//		TimestampFormat:time.RFC3339Nano,
		//TimestampFormat: "2006-01-02T15:04:05.000000000",
		TimestampFormat: "01-02 15:04:05.000",
	}
	// log.SetLevel(logrus.DebugLevel)
	log.WithFields(logrus.Fields{"package": "moleculer", "file": "moleculer"})

	// //	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&logrus.TextFormatter{
	// 	FullTimestamp: true,
	// 	//		TimestampFormat:time.RFC3339Nano,
	// 	//TimestampFormat: "2006-01-02T15:04:05.000000000",
	// 	TimestampFormat: "01-02 15:04:05.000",
	// })
	// log.SetLevel(logrus.DebugLevel)
	// log.WithFields(logrus.Fields{"package": "moleculer", "file": "go"})
	// log.Logger.SetLevel(logrus.DebugLevel)
	//
	//	// Output to stderr instead of stdout, could also be a file.
	//	logrus.SetOutput(os.Stderr)
	//
	//	// Only log the warning severity or above.
	//	logrus.SetLevel(logrus.WarnLevel)
}

//Create create a moleculer object
func Create() {

}

// Fn1 : just a test function
func Fn1(a int) int {
	// log.WithFields(log.Fields{
	// 	"class": "moleculer",
	// }).Info("Fn1 a = [%d]", a)
	// log.Info("AAAA")
	log.Printf("Fn1 a = [%d]", a)
	return a + 1
}
