package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// StartNATMapping attempts lightweight NAT-PMP and UPnP-IGD UDP port mapping for quicPort
// in the background and renews every 25 minutes until ctx is canceled or stop() is called.
func StartNATMapping(ctx context.Context, quicPort int) (stop func()) {
	if quicPort <= 0 {
		return func() {}
	}
	mapCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	var mu sync.Mutex
	var activeMethod string
	var upnpCtrlURL string
	var upnpServiceType string
	var pmpGateway net.IP

	cleanup := func() {
		mu.Lock()
		method := activeMethod
		ctrl := upnpCtrlURL
		svc := upnpServiceType
		gw := pmpGateway
		activeMethod = ""
		mu.Unlock()

		switch method {
		case "upnp":
			if ctrl != "" && svc != "" {
				delCtx, delCancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
				_ = upnpDeletePortMapping(delCtx, ctrl, svc, quicPort)
				delCancel()
			}
		case "nat-pmp":
			if gw != nil {
				_ = natPMPMapUDP(gw, quicPort, 0)
			}
		}
	}

	stopFn := func() {
		once.Do(func() {
			cancel()
			cleanup()
		})
	}

	go func() {
		loggedSuccess := false
		tryMap := func() {
			attemptCtx, attemptCancel := context.WithTimeout(mapCtx, 4*time.Second)
			defer attemptCancel()

			// 1. Try UPnP-IGD first (most common on home/SOHO routers)
			if ctrl, svc, err := upnpMapUDP(attemptCtx, quicPort, 3600); err == nil {
				mu.Lock()
				activeMethod = "upnp"
				upnpCtrlURL = ctrl
				upnpServiceType = svc
				mu.Unlock()
				if !loggedSuccess {
					loggedSuccess = true
					log.Printf("[p2p] NAT port mapping active via UPnP-IGD (UDP :%d)", quicPort)
				}
				return
			}

			// 2. Fallback to NAT-PMP (UDP :5351 on local gateway candidates)
			if gw, err := discoverAndMapNATPMP(quicPort, 3600); err == nil {
				mu.Lock()
				activeMethod = "nat-pmp"
				pmpGateway = gw
				mu.Unlock()
				if !loggedSuccess {
					loggedSuccess = true
					log.Printf("[p2p] NAT port mapping active via NAT-PMP (UDP :%d via %s)", quicPort, gw.String())
				}
			}
		}

		tryMap()
		ticker := time.NewTicker(25 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-mapCtx.Done():
				return
			case <-ticker.C:
				tryMap()
			}
		}
	}()

	return stopFn
}

// --- NAT-PMP (RFC 6886) ---

func discoverAndMapNATPMP(port int, lifetimeSec uint32) (net.IP, error) {
	candidates := candidateGatewayIPs()
	for _, gw := range candidates {
		if err := natPMPMapUDP(gw, port, lifetimeSec); err == nil {
			return gw, nil
		}
	}
	return nil, fmt.Errorf("no NAT-PMP gateway responded")
}

func natPMPMapUDP(gw net.IP, port int, lifetimeSec uint32) error {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: gw, Port: 5351})
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(700 * time.Millisecond))

	var req [12]byte
	req[0] = 0 // version 0
	req[1] = 1 // op: map UDP
	binary.BigEndian.PutUint16(req[4:6], uint16(port))
	binary.BigEndian.PutUint16(req[6:8], uint16(port))
	binary.BigEndian.PutUint32(req[8:12], lifetimeSec)

	if _, err := conn.Write(req[:]); err != nil {
		return err
	}
	var resp [16]byte
	n, err := conn.Read(resp[:])
	if err != nil || n < 16 {
		return fmt.Errorf("nat-pmp short/timeout response")
	}
	if resp[1] != 129 {
		return fmt.Errorf("nat-pmp unexpected opcode: %d", resp[1])
	}
	resCode := binary.BigEndian.Uint16(resp[2:4])
	if resCode != 0 {
		return fmt.Errorf("nat-pmp error code: %d", resCode)
	}
	return nil
}

func candidateGatewayIPs() []net.IP {
	var out []net.IP
	seen := make(map[string]struct{})
	add := func(ip net.IP) {
		ip4 := ip.To4()
		if ip4 == nil {
			return
		}
		s := ip4.String()
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, ip4)
		}
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 == nil || len(ipNet.Mask) != 4 || ip4.IsLinkLocalUnicast() {
				continue
			}
			netBase := ip4.Mask(ipNet.Mask)
			gw1 := net.IPv4(netBase[0], netBase[1], netBase[2], netBase[3]+1)
			gw254 := net.IPv4(netBase[0], netBase[1], netBase[2], 254)
			if !gw1.Equal(ip4) {
				add(gw1)
			}
			if !gw254.Equal(ip4) {
				add(gw254)
			}
		}
	}
	return out
}

// --- UPnP-IGD (SSDP + SOAP) ---

func upnpMapUDP(ctx context.Context, port int, leaseSec int) (ctrlURL string, svcType string, err error) {
	loc, st, err := ssdpDiscoverIGD(ctx)
	if err != nil {
		return "", "", err
	}
	ctrlURL, svcType, err = fetchIGDControlURL(ctx, loc, st)
	if err != nil {
		return "", "", err
	}
	localIP, err := outboundIPForURL(ctrlURL)
	if err != nil {
		return "", "", err
	}

	soapBody := fmt.Sprintf(`<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:AddPortMapping xmlns:u="%s">
      <NewRemoteHost></NewRemoteHost>
      <NewExternalPort>%d</NewExternalPort>
      <NewProtocol>UDP</NewProtocol>
      <NewInternalPort>%d</NewInternalPort>
      <NewInternalClient>%s</NewInternalClient>
      <NewEnabled>1</NewEnabled>
      <NewPortMappingDescription>Gens-Relay QUIC</NewPortMappingDescription>
      <NewLeaseDuration>%d</NewLeaseDuration>
    </u:AddPortMapping>
  </s:Body>
</s:Envelope>`, svcType, port, port, localIP, leaseSec)

	if err := postSOAP(ctx, ctrlURL, svcType, "AddPortMapping", soapBody); err != nil {
		return "", "", err
	}
	return ctrlURL, svcType, nil
}

func upnpDeletePortMapping(ctx context.Context, ctrlURL, svcType string, port int) error {
	soapBody := fmt.Sprintf(`<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:DeletePortMapping xmlns:u="%s">
      <NewRemoteHost></NewRemoteHost>
      <NewExternalPort>%d</NewExternalPort>
      <NewProtocol>UDP</NewProtocol>
    </u:DeletePortMapping>
  </s:Body>
</s:Envelope>`, svcType, port)
	return postSOAP(ctx, ctrlURL, svcType, "DeletePortMapping", soapBody)
}

func postSOAP(ctx context.Context, ctrlURL, svcType, action, body string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ctrlURL, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", `text/xml; charset="utf-8"`)
	req.Header.Set("SOAPAction", fmt.Sprintf(`"%s#%s"`, svcType, action))
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("upnp %s HTTP %d", action, resp.StatusCode)
	}
	return nil
}

func ssdpDiscoverIGD(ctx context.Context) (location string, serviceType string, err error) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return "", "", err
	}
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() {
		_ = conn.Close()
	})
	defer stopClose()
	_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))

	mcast := &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 1900}
	targets := []string{
		"urn:schemas-upnp-org:service:WANIPConnection:1",
		"urn:schemas-upnp-org:service:WANPPPConnection:1",
		"urn:schemas-upnp-org:service:WANIPConnection:2",
	}
	for _, st := range targets {
		msg := fmt.Sprintf("M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 1\r\nST: %s\r\n\r\n", st)
		_, _ = conn.WriteToUDP([]byte(msg), mcast)
	}

	buf := make([]byte, 4096)
	for {
		if ctx.Err() != nil {
			return "", "", ctx.Err()
		}
		n, _, rErr := conn.ReadFromUDP(buf)
		if rErr != nil {
			return "", "", fmt.Errorf("no UPnP-IGD device found")
		}
		resp := string(buf[:n])
		loc := extractHeader(resp, "LOCATION")
		st := extractHeader(resp, "ST")
		if loc != "" && (strings.Contains(st, "WANIPConnection") || strings.Contains(st, "WANPPPConnection")) {
			return loc, st, nil
		}
	}
}

func extractHeader(raw, key string) string {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		idx := strings.IndexByte(line, ':')
		if idx <= 0 {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(line[:idx]), key) {
			return strings.TrimSpace(line[idx+1:])
		}
	}
	return ""
}

func fetchIGDControlURL(ctx context.Context, locURL, preferredST string) (string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, locURL, nil)
	if err != nil {
		return "", "", err
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if err != nil {
		return "", "", err
	}

	type xmlService struct {
		ServiceType string `xml:"serviceType"`
		ControlURL  string `xml:"controlURL"`
	}
	var services []xmlService
	dec := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, tErr := dec.Token()
		if tErr != nil {
			break
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "service" {
			var svc xmlService
			if dec.DecodeElement(&svc, &se) == nil {
				services = append(services, svc)
			}
		}
	}

	base, err := url.Parse(locURL)
	if err != nil {
		return "", "", err
	}
	for _, svc := range services {
		if svc.ServiceType == preferredST ||
			strings.Contains(svc.ServiceType, "WANIPConnection") ||
			strings.Contains(svc.ServiceType, "WANPPPConnection") {
			rel, err := url.Parse(svc.ControlURL)
			if err != nil {
				continue
			}
			return base.ResolveReference(rel).String(), svc.ServiceType, nil
		}
	}
	return "", "", fmt.Errorf("UPnP WANIPConnection/WANPPPConnection service not found")
}

func outboundIPForURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	conn, err := net.DialTimeout("udp4", host, time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	addr := conn.LocalAddr().(*net.UDPAddr)
	return addr.IP.String(), nil
}
