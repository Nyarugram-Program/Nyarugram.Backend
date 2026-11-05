// Local UDP TURN/STUN service for the isolated Botgram phone test server.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/pion/logging"
	"github.com/pion/turn/v4"
)

type config struct {
	Address     string `json:"address"`
	Port        int    `json:"port"`
	MinPort     uint16 `json:"minPort"`
	MaxPort     uint16 `json:"maxPort"`
	Realm       string `json:"realm"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	AllowedCIDR string `json:"allowedCIDR"`
}

func main() {
	path := flag.String("config", "", "Private configuration file")
	probe := flag.Bool("probe", false, "Verify STUN, authenticated TURN data relay, and bad credentials")
	flag.Parse()
	if err := run(*path, *probe); err != nil {
		log.Fatal(err)
	}
}

func run(path string, probe bool) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var c config
	if err = json.Unmarshal(raw, &c); err != nil {
		return err
	}
	ip := net.ParseIP(c.Address)
	_, allowed, err := net.ParseCIDR(c.AllowedCIDR)
	if err != nil || ip == nil || ip.To4() == nil || !allowed.Contains(ip) || c.Port < 1024 || c.Port > 65535 || c.MinPort < 1024 || c.MaxPort < c.MinPort || c.MaxPort == 65535 || c.Realm == "" || c.Username == "" || len(c.Password) < 24 {
		return fmt.Errorf("invalid local TURN configuration")
	}
	if probe {
		return verify(c)
	}
	listener, err := net.ListenPacket("udp4", net.JoinHostPort(c.Address, fmt.Sprint(c.Port)))
	if err != nil {
		return err
	}
	defer listener.Close()
	key := turn.GenerateAuthKey(c.Username, c.Realm, c.Password)
	server, err := turn.NewServer(turn.ServerConfig{
		Realm: c.Realm,
		AuthHandler: func(username, realm string, source net.Addr) ([]byte, bool) {
			addr, ok := source.(*net.UDPAddr)
			return key, ok && allowed.Contains(addr.IP) && username == c.Username && realm == c.Realm
		},
		LoggerFactory: logging.NewDefaultLoggerFactory(),
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: listener,
			RelayAddressGenerator: &turn.RelayAddressGeneratorPortRange{
				RelayAddress: ip, Address: c.Address, MinPort: c.MinPort, MaxPort: c.MaxPort, MaxRetries: 100,
			},
			PermissionHandler: func(_ net.Addr, peer net.IP) bool { return allowed.Contains(peer) && !peer.IsMulticast() },
		}},
	})
	if err != nil {
		return err
	}
	defer server.Close()
	log.Printf("TURN/STUN UDP ready at %s; relay ports %d-%d; peers restricted to %s", listener.LocalAddr(), c.MinPort, c.MaxPort, allowed)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	defer signal.Stop(stop)
	<-stop
	return nil
}

func client(c config, password string) (*turn.Client, net.PacketConn, error) {
	conn, err := net.ListenPacket("udp4", net.JoinHostPort(c.Address, "0"))
	if err != nil {
		return nil, nil, err
	}
	addr := net.JoinHostPort(c.Address, fmt.Sprint(c.Port))
	cl, err := turn.NewClient(&turn.ClientConfig{STUNServerAddr: addr, TURNServerAddr: addr, Conn: conn, Username: c.Username, Password: password, Realm: c.Realm, LoggerFactory: logging.NewDefaultLoggerFactory()})
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if err = cl.Listen(); err != nil {
		cl.Close()
		conn.Close()
		return nil, nil, err
	}
	return cl, conn, nil
}

func verify(c config) error {
	cl, socket, err := client(c, c.Password)
	if err != nil {
		return err
	}
	defer socket.Close()
	defer cl.Close()
	mapped, err := cl.SendBindingRequest()
	if err != nil {
		return fmt.Errorf("STUN: %w", err)
	}
	fmt.Printf("PASS: STUN binding mapped=%s\n", mapped)
	relay, err := cl.Allocate()
	if err != nil {
		return fmt.Errorf("TURN allocation: %w", err)
	}
	defer relay.Close()
	addr := relay.LocalAddr().(*net.UDPAddr)
	if !addr.IP.Equal(net.ParseIP(c.Address)) || addr.Port < int(c.MinPort) || addr.Port > int(c.MaxPort) {
		return fmt.Errorf("invalid relay endpoint")
	}
	peer, err := net.ListenPacket("udp4", net.JoinHostPort(c.Address, "0"))
	if err != nil {
		return err
	}
	defer peer.Close()
	relay.SetDeadline(time.Now().Add(5 * time.Second))
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	message := []byte("botgram-turn-relay-check")
	if _, err = relay.WriteTo(message, peer.LocalAddr()); err != nil {
		return err
	}
	buffer := make([]byte, 2048)
	n, from, err := peer.ReadFrom(buffer)
	if err != nil {
		return err
	}
	if !bytes.Equal(buffer[:n], message) || from.String() != relay.LocalAddr().String() {
		return fmt.Errorf("outbound relay mismatch")
	}
	if _, err = peer.WriteTo(buffer[:n], from); err != nil {
		return err
	}
	n, from, err = relay.ReadFrom(buffer)
	if err != nil {
		return err
	}
	if !bytes.Equal(buffer[:n], message) || from.String() != peer.LocalAddr().String() {
		return fmt.Errorf("inbound relay mismatch")
	}
	fmt.Printf("PASS: authenticated TURN allocation %s and bidirectional UDP relay\n", addr)
	bad, badSocket, err := client(c, c.Password+"-invalid")
	if err != nil {
		return err
	}
	defer badSocket.Close()
	defer bad.Close()
	if unexpected, allocErr := bad.Allocate(); allocErr == nil {
		unexpected.Close()
		return fmt.Errorf("invalid credentials accepted")
	}
	fmt.Println("PASS: invalid TURN credentials rejected")
	return nil
}
