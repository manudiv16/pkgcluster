package pkgcluster

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/net/ipv4"
)

// ---------------------------------------------------------------------------
// Gossip strategy
// ---------------------------------------------------------------------------

// GossipStrategy uses UDP multicast to discover cluster nodes. It is a Go
// clone of libcluster's Cluster.Strategy.Gossip.
//
// Each node periodically sends heartbeat messages containing its node address
// to a multicast group. Nodes discovered via heartbeat are connected.
//
// Config keys:
//   - "port" (int) — UDP port (default: 45892)
//   - "if_addr" (string) — interface address to bind (default: "0.0.0.0")
//   - "multicast_addr" (string) — multicast group (default: "233.252.1.32")
//   - "multicast_ttl" (int) — multicast TTL (default: 1)
//   - "multicast_if" (string) — multicast interface IP (optional)
//   - "secret" (string) — optional encryption key for heartbeats
//   - "broadcast_only" (bool) — use broadcast instead of multicast (default: false)
//   - "node_addr" (string) — this node's address to announce in heartbeat
type GossipStrategy struct {
	state State
}

// NewGossipStrategy creates a new GossipStrategy.
func NewGossipStrategy(s State) (Strategy, error) {
	return &GossipStrategy{state: s}, nil
}

// Name returns "gossip".
func (g *GossipStrategy) Name() string { return "gossip" }

// Run listens for gossip heartbeats and periodically announces this node.
func (g *GossipStrategy) Run(ctx context.Context, state State) error {
	port := IntConfig(state, "port", 45892)
	ifAddr := StringConfig(state, "if_addr", "0.0.0.0")
	mcastAddrStr := StringConfig(state, "multicast_addr", "233.252.1.32")
	ttl := IntConfig(state, "multicast_ttl", 1)
	broadcastOnly := BoolConfig(state, "broadcast_only", false)
	secret := StringConfig(state, "secret", "")
	nodeAddr := StringConfig(state, "node_addr", "")

	localIP := net.ParseIP(ifAddr)
	if localIP == nil {
		localIP = net.IPv4zero
	}
	mcastIP := net.ParseIP(mcastAddrStr)
	if mcastIP == nil {
		return fmt.Errorf("invalid multicast address: %s", mcastAddrStr)
	}

	// Open UDP socket.
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: localIP, Port: port})
	if err != nil {
		return fmt.Errorf("gossip: failed to listen on %s:%d: %w", ifAddr, port, err)
	}
	defer conn.Close()

	// Wrap in ipv4.PacketConn for multicast control.
	pc := ipv4.NewPacketConn(conn)
	defer pc.Close()

	if !broadcastOnly {
		var ifAddrToUse net.IP
		if mcastIf := StringConfig(state, "multicast_if", ""); mcastIf != "" {
			ifAddrToUse = net.ParseIP(mcastIf)
		}
		iface, err := interfaceForIP(ifAddrToUse)
		if err != nil {
			globalLogger.Warn("gossip[%s]: could not find interface: %v", state.Name, err)
		} else {
			if err := pc.JoinGroup(iface, &net.UDPAddr{IP: mcastIP}); err != nil {
				globalLogger.Warn("gossip[%s]: multicast join failed, falling back to listen-only: %v",
					state.Name, err)
			}
		}
	}

	// Set multicast TTL.
	if err := pc.SetMulticastTTL(ttl); err != nil {
		globalLogger.Debug("gossip[%s]: could not set TTL: %v", state.Name, err)
	}

	globalLogger.Debug("gossip[%s]: listening on %s:%d, multicast %s, ttl=%d",
		state.Name, ifAddr, port, mcastAddrStr, ttl)

	// Heartbeat codec.
	codec := newHeartbeatCodec(secret)

	// Channel for discovered node addresses.
	type event struct {
		addr string
		err  error
	}
	events := make(chan event, 64)

	// Read goroutine — reads UDP packets and extracts addresses.
	go func() {
		buf := make([]byte, 1500)
		for {
			n, _, src, err := pc.ReadFrom(buf)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case events <- event{err: err}:
				default:
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			addr := codec.extractAddress(buf[:n])
			if addr != "" && addr != nodeAddr {
				select {
				case events <- event{addr: addr}:
				default:
				}
			}
			// Also use the source IP as a fallback address.
			if addr == "" {
				if src != nil {
					select {
					case events <- event{addr: src.String()}:
					default:
					}
				}
			}
		}
	}()

	// Announce ticker — random interval 1-5s like libcluster.
	initialDelay := time.Duration(1+time.Now().UnixNano()%5000) * time.Millisecond
	announceTicker := time.NewTicker(initialDelay)
	defer announceTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case e := <-events:
			if e.err != nil {
				globalLogger.Debug("gossip[%s]: read error: %v", state.Name, e.err)
				continue
			}
			if e.addr == "" {
				continue
			}
			// Connect the discovered node if not already connected.
			current, _ := state.ListNodes(ctx)
			if !contains(current, e.addr) {
				globalLogger.Debug("gossip[%s]: discovered %s", state.Name, e.addr)
				if err := state.Connect(ctx, e.addr); err != nil {
					globalLogger.Warn("gossip[%s]: connect %s failed: %v",
						state.Name, e.addr, err)
				}
			}

		case <-announceTicker.C:
			// Re-roll the interval (like libcluster's random heartbeat).
			announceTicker.Reset(time.Duration(1+time.Now().UnixNano()%5000) * time.Millisecond)

			msg := codec.pack(nodeAddr)
			dst := &net.UDPAddr{IP: mcastIP, Port: port}
			if broadcastOnly {
				dst.IP = net.IPv4bcast
			}
			if _, err := pc.WriteTo(msg, nil, dst); err != nil {
				globalLogger.Debug("gossip[%s]: announce error: %v", state.Name, err)
			}
		}
	}
}

// interfaceForIP returns the network interface that has the given IP address.
// If ip is nil, returns nil (caller handles the fallback).
func interfaceForIP(ip net.IP) (*net.Interface, error) {
	if ip == nil {
		return nil, nil
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var addrIP net.IP
			switch v := a.(type) {
			case *net.IPNet:
				addrIP = v.IP
			case *net.IPAddr:
				addrIP = v.IP
			}
			if addrIP != nil && addrIP.Equal(ip) {
				return &iface, nil
			}
		}
	}
	return nil, fmt.Errorf("no interface with IP %s", ip)
}

// ---------------------------------------------------------------------------
// Heartbeat protocol
// ---------------------------------------------------------------------------

type heartbeatCodec struct {
	secret []byte
	key    []byte
}

func newHeartbeatCodec(secret string) *heartbeatCodec {
	h := &heartbeatCodec{}
	if secret != "" {
		h.secret = []byte(secret)
		key := sha256.Sum256([]byte(secret))
		h.key = key[:]
	}
	return h
}

func (h *heartbeatCodec) pack(nodeAddr string) []byte {
	payload := []byte("heartbeat::" + nodeAddr)
	if h.secret == nil {
		// Plaintext: 2-byte length prefix + payload.
		msg := make([]byte, 2+len(payload))
		binary.BigEndian.PutUint16(msg[:2], uint16(len(payload)))
		copy(msg[2:], payload)
		return msg
	}
	// Encrypted with AES-256-CBC (like libcluster).
	padded := pkcs7Pad(payload)
	iv := make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		return nil
	}
	block, err := aes.NewCipher(h.key)
	if err != nil {
		return nil
	}
	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(padded))
	mode.CryptBlocks(ciphertext, padded)
	// Format: 2-byte length(16+len(ciphertext)) + iv(16) + ciphertext.
	msg := make([]byte, 2+16+len(ciphertext))
	binary.BigEndian.PutUint16(msg[:2], uint16(16+len(ciphertext)))
	copy(msg[2:], iv)
	copy(msg[2+16:], ciphertext)
	return msg
}

func (h *heartbeatCodec) extractAddress(raw []byte) string {
	if len(raw) < 3 {
		return ""
	}
	length := int(binary.BigEndian.Uint16(raw[:2]))
	if length < 11 || length > len(raw)-2 {
		return ""
	}
	body := raw[2 : 2+length]

	if h.secret == nil {
		return parseHeartbeat(body)
	}
	// Decrypt AES-256-CBC.
	if len(body) < aes.BlockSize {
		return ""
	}
	iv := body[:aes.BlockSize]
	ciphertext := body[aes.BlockSize:]
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return ""
	}
	block, err := aes.NewCipher(h.key)
	if err != nil {
		return ""
	}
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)
	unpadded, err := pkcs7Unpad(plaintext)
	if err != nil {
		return ""
	}
	return parseHeartbeat(unpadded)
}

func parseHeartbeat(body []byte) string {
	prefix := []byte("heartbeat::")
	if len(body) < len(prefix) || string(body[:len(prefix)]) != string(prefix) {
		return ""
	}
	return strings.TrimSpace(string(body[len(prefix):]))
}

// ---------------------------------------------------------------------------
// PKCS7 padding helpers
// ---------------------------------------------------------------------------

func pkcs7Pad(data []byte) []byte {
	padding := aes.BlockSize - len(data)%aes.BlockSize
	out := make([]byte, len(data)+padding)
	copy(out, data)
	for i := len(data); i < len(out); i++ {
		out[i] = byte(padding)
	}
	return out
}

func pkcs7Unpad(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}
	padding := int(data[len(data)-1])
	if padding > aes.BlockSize || padding == 0 || padding > len(data) {
		return nil, fmt.Errorf("invalid padding")
	}
	for i := len(data) - padding; i < len(data); i++ {
		if int(data[i]) != padding {
			return nil, fmt.Errorf("invalid padding bytes")
		}
	}
	return data[:len(data)-padding], nil
}
