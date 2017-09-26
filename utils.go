package moleculer

import (
	"net"
	"os"
	"strings"
)

func _getLanIP() string {
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
		if !strings.HasPrefix(value, "127") && !strings.HasSuffix(value, ".1") {
			ret = value
			return ret
		}
	}
	return ret
}
