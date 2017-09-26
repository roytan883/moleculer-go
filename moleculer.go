package moleculer

import (
	"strings"
	"sync"
	"time"

	logrus "github.com/Sirupsen/logrus"
	nats "github.com/nats-io/go-nats"
	"github.com/roytan883/moleculer/protocol"

	jsoniter "github.com/json-iterator/go"
	stringsUtils "github.com/shomali11/util/strings"
)

var log *logrus.Entry

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
	Services map[string]Service
}

//ServiceBroker ...
type ServiceBroker struct {
	config         *ServiceBrokerConfig
	con            *nats.Conn
	selfInfo       *protocol.MsInfoNode
	selfInfoString string
	nodeInfos      *sync.Map //map[string]interface{}
	nodeServices   *sync.Map //map[string]interface{}
	stoped         chan int
}

//NewServiceBroker ...
func NewServiceBroker(config *ServiceBrokerConfig) (*ServiceBroker, error) {
	serviceBroker := &ServiceBroker{
		con:       nil,
		config:    config,
		nodeInfos: &sync.Map{},
		stoped:    make(chan int, 1),
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

const json = `{"name":{"first":"Janet","last":"Prichard"},"age":47}`

//Start ...
func (broker *ServiceBroker) Start() (err error) {

	// jsoniter.Marshal(&data)

	// value := gjson.Get(json, "name.last")
	// gjson.Parse(json)
	// println(value.String())

	if broker.con != nil {
		return
	}
	natsOpts := nats.DefaultOptions
	natsOpts.AllowReconnect = true
	natsOpts.ReconnectWait = 2 * time.Second            //2s
	natsOpts.MaxReconnect = 30 * 60 * 24 * 30 * 12 * 10 //10 years
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
	var Response = "MOL.RESPONSE." + broker.config.NodeID
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
	return nil
}

func (broker *ServiceBroker) _startHeartbeatTimer() {
	for {
		select {
		case <-broker.stoped:
			return

		case <-time.After(time.Second * 3):
			broker._broadcastHeartbeat()

			// case <-time.After(time.Second * 15):
			// 	println("10s timer")
		}
	}
}

//CallOptions ...
type CallOptions struct {
	timeout    time.Duration
	retryCount uint64
	nodeID     string
	meta       interface{}
}

//Call ...
func (broker *ServiceBroker) Call(service string, action string, req []byte, opts CallOptions) (res []byte, err error) {
	return nil, nil
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
func (broker *ServiceBroker) _onRequest(msg *nats.Msg) {
	log.Info("NATS _onRequest: ", string(msg.Data))
	jsonObj := &protocol.MsRequest{}
	err := jsoniter.Unmarshal(msg.Data, jsonObj)
	if err != nil {
		log.Error("NATS _onRequest parse err: ", err)
		return
	}
	actionNames := strings.Split(jsonObj.Action, ".")
	if len(actionNames) != 2 {
		log.Error("NATS _onRequest find action err: ", actionNames)
		return
	}
	var serviceName = actionNames[0]
	var actionName = actionNames[1]
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
}
func (broker *ServiceBroker) _onResponse(msg *nats.Msg) {
	log.Info("NATS _onResponse: ", string(msg.Data))
	return
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
	var data protocol.MsInfoNode
	jsoniter.Unmarshal(msg.Data, &data)
	log.Info("NATS _onInfo: data", data)
	broker.nodeInfos.Store(data.Sender, data)
	return
}
func (broker *ServiceBroker) _onInfoTargetted(msg *nats.Msg) {
	s := string(msg.Data)
	log.Info("NATS _onInfoTargetted: ", s)
	var data protocol.MsInfoNode
	jsoniter.Unmarshal(msg.Data, &data)
	log.Info("NATS _onInfoTargetted data: ", data)
	broker.nodeInfos.Store(data.Sender, data)
	return
}
func (broker *ServiceBroker) _onHeartbeat(msg *nats.Msg) {
	// log.Info("NATS _onHeartbeat: ", string(msg.Data))
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

func initLog() {
	//	// Log as JSON instead of the default ASCII formatter.
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		//		TimestampFormat:time.RFC3339Nano,
		//TimestampFormat: "2006-01-02T15:04:05.000000000",
		TimestampFormat: "01-02 15:04:05.000",
	})
	logrus.SetLevel(logrus.DebugLevel)
	log = logrus.WithFields(logrus.Fields{"package": "moleculer", "file": "go"})
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
