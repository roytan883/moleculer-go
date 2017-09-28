# moleculer-go
This is Moleculer implementation for go (>=1.9.x). Currently only support [NATS](http://nats.io) as transporter.


# moleculer(nodejs) info :
Moleculer is a fast, modern and powerful microservices framework for NodeJS (>= v6.x).

**Moleculer**: [https://github.com/ice-services/moleculer](https://github.com/ice-services/moleculer)

**Website**: [https://moleculer.services](https://moleculer.services)

**Documentation**: [https://moleculer.services/docs](https://moleculer.services/docs)

# How to use, refer to example :

> run:
```
go run .\examples\moleculer-go-demo.go -s nats://192.168.1.69:12008
```
> output:
```
INFO[09-28 14:37:19.580887] broker.Call demoService.actionA start
INFO[09-28 14:37:19.581886] run actionA, req.Params = map[arg2:123 arg1:aaa]
INFO[09-28 14:37:19.582886] broker.Call demoService.actionA end, res: map[res1:AAA res2:123]
INFO[09-28 14:37:19.582886] broker.Call demoService.actionA end, err: <nil>
INFO[09-28 14:37:19.582886] broker.Call demoService.actionB start
INFO[09-28 14:37:19.582886] run actionB, req.Params = map[arg1:bbb arg2:456]
INFO[09-28 14:37:19.583886] broker.Call demoService.actionB end, res: map[res2:456 res1:BBB]
INFO[09-28 14:37:19.583886] broker.Call demoService.actionB end, err: <nil>
INFO[09-28 14:37:19.583886] broker.Emit user.create start
INFO[09-28 14:37:19.583886] broker.Emit user.create end, err: <nil>
INFO[09-28 14:37:19.583886] broker.Broadcast user.delete start
INFO[09-28 14:37:19.583886] broker.Broadcast user.delete end, err: <nil>
INFO[09-28 14:37:19.583886] run onEventUserCreate, req.Data = map[user:userA status:create]
INFO[09-28 14:37:19.583886] run onEventUserDelete, req.Data = map[user:userB status:delete]
```
# Performance 
> ENV: one receiver process and one sender process on WIN10(i7-2600), NATS Server on Ubuntu Server 14.04(i7-4790K)
```
broker.Call demoService.bench goroutineNum[1] callCount[10000] use[6.0100862s] req/s[1663] minLatency[999.9µs] maxLatency[5.0021ms]
broker.Call demoService.bench goroutineNum[5] callCount[50000] use[5.9192496s] req/s[8447] minLatency[999.9µs] maxLatency[10.0015ms]
broker.Call demoService.bench goroutineNum[10] callCount[100000] use[6.3480519s] req/s[15752] minLatency[1.0002ms] maxLatency[12.0096ms]
broker.Call demoService.bench goroutineNum[50] callCount[500000] use[10.052548s] req/s[49738] minLatency[1ms] maxLatency[25.0064ms]
```

# Status :

## v0.1.0
- [x] MOL.DISCOVER
- [x] MOL.DISCOVER.`nodeID`
- [x] MOL.INFO
- [x] MOL.INFO.nodeID
- [x] MOL.HEARTBEAT
- [x] MOL.REQ.`nodeID`
- [ ] MOL.REQB.`action`
- [x] MOL.RES.`nodeID`
- [x] MOL.EVENT.`nodeID`
- [ ] MOL.EVENTB.`event`
- [x] MOL.PING
- [x] MOL.PING.`nodeID`
- [x] MOL.PONG.`nodeID`
- [x] MOL.DISCONNECT


