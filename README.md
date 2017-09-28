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


