package v2rayhttpupgrade

import (
	std_bufio "bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestClientUpgradeConnectionCleanup(t *testing.T) {
	const accepted = "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"
	for _, test := range []struct {
		name     string
		response string
		wantErr  string
	}{
		{name: "write_failure", wantErr: "closed pipe"},
		{name: "malformed_response", response: "not HTTP\r\n\r\n", wantErr: "malformed HTTP"},
		// Neither rejected response supplies the advertised body. Cleanup must
		// return without waiting for it, including after closing the connection.
		{name: "rejected_content_length", response: "HTTP/1.1 403 Forbidden\r\nContent-Length: 4096\r\n\r\n", wantErr: "unexpected status"},
		{name: "rejected_chunked", response: "HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n", wantErr: "unexpected status"},
		{name: "invalid_connection", response: "HTTP/1.1 101 Switching Protocols\r\nConnection: keep-alive\r\nUpgrade: websocket\r\n\r\n", wantErr: "unexpected status"},
		{name: "invalid_upgrade", response: "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: other\r\n\r\n", wantErr: "unexpected status"},
		{name: "success", response: accepted},
		{name: "success_with_buffered_data", response: accepted + "hello"},
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
				requestURL: url.URL{Scheme: "http", Host: "example.org", Path: "/"},
				headers:    make(http.Header),
				host:       "example.org",
			}
			serverDone := make(chan error, 1)
			go func() {
				if test.response == "" {
					serverDone <- peer.Close()
					return
				}
				if _, err := http.ReadRequest(std_bufio.NewReader(peer)); err != nil {
					serverDone <- err
					return
				}
				_, err := io.WriteString(peer, test.response)
				serverDone <- err
			}()
			type result struct {
				conn net.Conn
				err  error
			}
			done := make(chan result, 1)
			go func() {
				conn, err := client.DialContext(t.Context())
				done <- result{conn, err}
			}()
			var got result
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("upgrade waited for a response body instead of returning")
			}
			if test.wantErr != "" {
				if got.err == nil || !strings.Contains(got.err.Error(), test.wantErr) || got.conn != nil {
					t.Fatalf("DialContext() = (%v, %v), want nil connection and %q", got.conn, got.err, test.wantErr)
				}
			} else {
				if got.err != nil || got.conn == nil {
					t.Fatalf("DialContext() = (%v, %v)", got.conn, got.err)
				}
				if raw.closes.Load() != 0 {
					t.Fatal("successful upgrade closed the connection")
				}
				if err := got.conn.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if count := raw.closes.Load(); count != 1 {
				t.Fatalf("underlying Close called %d times, want 1", count)
			}
			if count := raw.readsAfterClose.Load(); count != 0 {
				t.Fatalf("attempted to drain the rejected response after closing: %d reads", count)
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
	closes          atomic.Int32
	readsAfterClose atomic.Int32
}

func (c *upgradeTestConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

func (c *upgradeTestConn) Read(p []byte) (int, error) {
	if c.closes.Load() != 0 {
		c.readsAfterClose.Add(1)
	}
	return c.Conn.Read(p)
}
