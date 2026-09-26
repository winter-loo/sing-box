package v2raywebsocket

import (
	std_bufio "bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestClientUpgradeConnectionCleanup(t *testing.T) {
	for _, mode := range []string{"immediate", "early_data_path", "early_data_header"} {
		t.Run(mode, func(t *testing.T) {
			for _, test := range []struct {
				name     string
				response string
			}{
				{name: "write_failure"},
				{name: "malformed_response", response: "not HTTP\r\n\r\n"},
				{name: "rejected_response", response: "HTTP/1.1 403 Forbidden\r\nContent-Length: 4096\r\n\r\n"},
				{name: "success"},
			} {
				t.Run(test.name, func(t *testing.T) {
					local, peer := net.Pipe()
					t.Cleanup(func() {
						local.Close()
						peer.Close()
					})
					raw := &upgradeTestConn{Conn: local}
					client := &Client{
						dialer:     &upgradeTestDialer{conn: raw},
						requestURL: url.URL{Scheme: "ws", Host: "example.org", Path: "/"},
						headers:    make(http.Header),
					}
					if mode != "immediate" {
						client.maxEarlyData = 1
					}
					if mode == "early_data_header" {
						client.earlyDataHeaderName = "Sec-WebSocket-Protocol"
					}
					serverDone := make(chan error, 1)
					go func() {
						if test.name == "write_failure" {
							serverDone <- peer.Close()
							return
						}
						request, err := http.ReadRequest(std_bufio.NewReader(peer))
						if err != nil {
							serverDone <- err
							return
						}
						response := test.response
						if test.name == "success" {
							accept := sha1.Sum([]byte(request.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
							response = "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(accept[:]) + "\r\n\r\n"
						}
						_, err = io.WriteString(peer, response)
						if err == nil && test.name == "success" {
							// Receive the close frame; net.Pipe writes are synchronous.
							_, err = io.Copy(io.Discard, peer)
						}
						serverDone <- err
					}()
					type result struct {
						conn net.Conn
						err  error
					}
					done := make(chan result, 1)
					go func() {
						conn, err := client.DialContext(t.Context())
						if err == nil && mode != "immediate" {
							_, err = conn.Write([]byte("x"))
						}
						done <- result{conn, err}
					}()
					var got result
					select {
					case got = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("upgrade did not return")
					}
					if test.name == "success" {
						if got.err != nil || got.conn == nil {
							t.Fatalf("upgrade = (%v, %v)", got.conn, got.err)
						}
						if raw.closes.Load() != 0 {
							t.Fatal("successful upgrade closed the connection")
						}
						if err := got.conn.Close(); err != nil {
							t.Fatal(err)
						}
					} else if got.err == nil {
						t.Fatal("expected an upgrade error")
					} else if mode == "immediate" && got.conn != nil {
						t.Fatal("failed immediate upgrade returned a connection")
					}
					// Check before caller cleanup: closing the returned lazy
					// connection here would conceal an early-data upgrade leak.
					if count := raw.closes.Load(); count != 1 {
						t.Fatalf("underlying Close called %d times, want 1", count)
					}
					select {
					case err := <-serverDone:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("response writer did not finish")
					}
				})
			}
		})
	}
}

type upgradeTestDialer struct {
	N.Dialer
	conn net.Conn
}

func (d *upgradeTestDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return d.conn, nil
}

type upgradeTestConn struct {
	net.Conn
	closes atomic.Int32
}

func (c *upgradeTestConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}
