package sip

import (
	"fmt"
	"net"
)

// LocalIPs returns the unicast addresses assigned to the machine's interfaces.
func LocalIPs() ([]net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list network interfaces: %w", err)
	}
	var ips []net.IP
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("addresses of %s: %w", iface.Name, err)
		}
		for _, addr := range addrs {
			if ip := addrIP(addr); ip != nil {
				ips = append(ips, ip)
			}
		}
	}
	return ips, nil
}

// RequireLocal reports whether the host in a sip probe address is assigned to
// a local interface. A copied configuration must not start and probe an
// address that belongs to another machine.
func RequireLocal(addr string, locals []net.IP) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %s: %w", addr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("address %s is not an IP assigned to a local interface", host)
	}
	for _, local := range locals {
		if local.Equal(ip) {
			return nil
		}
	}
	return fmt.Errorf("address %s is not assigned to a local interface", ip)
}

func addrIP(addr net.Addr) net.IP {
	switch a := addr.(type) {
	case *net.IPNet:
		return a.IP
	case *net.IPAddr:
		return a.IP
	default:
		return nil
	}
}
