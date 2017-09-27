# moleculer-go
This is Moleculer implementation for go (>=1.9.x). Currently only support [NATS](http://nats.io) as transporter.


# moleculer(nodejs) info :
Moleculer is a fast, modern and powerful microservices framework for NodeJS (>= v6.x).

**Website**: [https://moleculer.services](https://moleculer.services)

**Documentation**: [https://moleculer.services/docs](https://moleculer.services/docs)

# How to use :

```
go run .\examples\moleculer-go-demo.go -s nats://192.168.1.69:12008
```

# Status :

## v0.1.0
- [x] register and discover Moleculer services 
- [x] send and receive Moleculer heartbeat 
- [x] sync Moleculer nodes status
- [x] handle Moleculer REQUEST 
- [x] send Moleculer REQUEST 
- [ ] Moleculer EVENT
  - [ ] event
  - [ ] broadcast
