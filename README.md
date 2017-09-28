# moleculer-go
This is Moleculer implementation for go (>=1.9.x). Currently only support [NATS](http://nats.io) as transporter.


# moleculer(nodejs) info :
Moleculer is a fast, modern and powerful microservices framework for NodeJS (>= v6.x).

**Moleculer**: [https://github.com/ice-services/moleculer](https://github.com/ice-services/moleculer)

**Website**: [https://moleculer.services](https://moleculer.services)

**Documentation**: [https://moleculer.services/docs](https://moleculer.services/docs)

# How to use :

```
go run .\examples\moleculer-go-demo.go -s nats://192.168.1.69:12008
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


