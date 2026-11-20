package main

import (
	"fmt"
	"net"
	"os"

	"github.com/Yuri666/systemd-transition-exporter/internal/sip"
)

func main() {
	addr := "10.255.98.75:5060"
	if len(os.Args) == 2 {
		addr = os.Args[1]
	} else if len(os.Args) > 2 {
		fmt.Fprintf(os.Stderr, "usage: sip-options [host:port]\n")
		os.Exit(2)
	}
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	conn, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	local, _ := conn.LocalAddr().(*net.UDPAddr)
	if local == nil {
		fmt.Fprintln(os.Stderr, "local address is not udp")
		os.Exit(1)
	}
	message, err := sip.Options("sip:"+addr, local.IP.String(), local.Port, 1, sip.MaxForwards("pcscf.service"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err = conn.Write(message.Bytes); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("sent %s -> %s call-id %s\n", local, remote, message.CallID)
}
