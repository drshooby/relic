package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// Line shapes taken from a real session. Addresses are the thing being removed;
// ports, player ids, and everything else must survive untouched.
func TestRedactIPs(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"NAT bound (the line that leaked)",
			"543.288 Net [Info]: NAT bound for client to 104.14.26.107:4955",
			"543.288 Net [Info]: NAT bound for client to <ip>:4955",
		},
		{
			"squad message from a peer",
			"389.182 Game [Info]: HandleSquadMessage from 173.255.10.20:4950 JOIN (host: 0)",
			"389.182 Game [Info]: HandleSquadMessage from <ip>:4950 JOIN (host: 0)",
		},
		{
			"two addresses on one line",
			"388.555 Game [Info]: Received introduction request from 68.171.5.9:4950 (reply to 47.149.8.3:37956)",
			"388.555 Game [Info]: Received introduction request from <ip>:4950 (reply to <ip>:37956)",
		},
		{
			"hub registration",
			"103.092 Game [Info]: Registered to hub: 201.20.30.40:6951, hub index = 11",
			"103.092 Game [Info]: Registered to hub: <ip>:6951, hub index = 11",
		},
		{
			"unspecified address is not sensitive but is still an address",
			"3.459 Net [Info]: Local address: 0.0.0.0",
			"3.459 Net [Info]: Local address: <ip>",
		},
		{
			// Not every address carries a port, so a port cannot be used to
			// recognise one.
			"bare address with no port",
			"100.000 Sys [Info]: Contacting 23.194.66.70 for content",
			"100.000 Sys [Info]: Contacting <ip> for content",
		},
		{
			"LAN address, also no port",
			"12.000 Net [Info]: Adapter bound to 192.168.1.67",
			"12.000 Net [Info]: Adapter bound to <ip>",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactLine(tc.in); got != tc.want {
				t.Errorf("redactLine()\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// Version strings and similar dotted numbers look like addresses but are not.
// Over-redacting would corrupt the archive, which is worse than a cosmetic miss.
func TestRedactLeavesNonAddressesAlone(t *testing.T) {
	unchanged := []string{
		"0.298 Sys [Diag]: Build Label: 2026.07.11.15.28 Retail Windows x64 [Stripped]",
		// The real line from EE.log. 4.1.1.0 is a syntactically valid address,
		// so only the word "Version" distinguishes it.
		"2.740 Phys [Info]: PhysX Core Version: 4.1.1.0",
		"0.301 Sys [Diag]: Build Unique ID: 1911257066",
		"549.101 Sys [Info]: Weapon in slot SUIT_SLOT with ID 6a5823060a0e66072802cf12 has gained 24635 XP",
		"533.704 Sys [Info]: VoidProjections: 5af65573f2f2eb9f0f19def3 gets reward /Lotus/StoreItems/Types/Recipes/WarframeRecipes/VorunaPrimeHelmetBlueprint",
		"549.097 Sys [Info]: SyndicateXP base for mission: 1108",
	}
	for _, s := range unchanged {
		if got := redactLine(s); got != s {
			t.Errorf("redactLine() modified a non-address line\n got: %q\nwant: %q", got, s)
		}
	}
}

// The whole point: no envelope reaching a sink may carry an address.
func TestEmittedEnvelopesCarryNoIPs(t *testing.T) {
	path := writeTempLog(t, testHeader+
		"1.000 Net [Info]: NAT bound for client to 104.14.26.107:4955\r\n"+
		"2.000 Game [Info]: HandleSquadMessage from 173.255.10.20:4950 JOIN (host: 0)\r\n")

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	for i, l := range sink.lines {
		if ipRe.MatchString(l) {
			t.Errorf("envelope %d still contains an address: %q", i, l)
		}
	}
	// The surrounding content must survive.
	joined := strings.Join(sink.lines, "\n")
	for _, want := range []string{"NAT bound for client to", ":4955", "HandleSquadMessage", "JOIN (host: 0)"} {
		if !strings.Contains(joined, want) {
			t.Errorf("redaction removed more than the address: %q missing", want)
		}
	}
}

// End-to-end guard: replaying the fixture must not yield a single address.
// The fixture deliberately contains address-bearing lines (with documentation-
// range addresses) so this covers the real line shapes, not just unit cases.
func TestFixtureReplayLeaksNoIPs(t *testing.T) {
	fixture, err := os.ReadFile("../../testdata/session_sample.log")
	if err != nil {
		t.Skip("fixture not present")
	}
	if !bytes.Contains(fixture, []byte("NAT bound")) {
		t.Fatal("fixture has no address-bearing lines; it no longer covers redaction")
	}

	path := writeTempLog(t, string(fixture))
	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	for i, l := range sink.lines {
		if versionContextRe.MatchString(l) {
			continue // version strings are a documented exception
		}
		if ipRe.MatchString(l) {
			t.Errorf("line %d leaked an address: %q", i, l)
		}
	}
}

func writeTempLog(t *testing.T, content string) string {
	t.Helper()
	path := t.TempDir() + "/EE.log"
	writeFile(t, path, content)
	return path
}
