package main

import "regexp"

// ipRe matches a dotted quad with range-checked octets. Bare `\d+\.\d+\.\d+\.\d+`
// would also swallow build labels like "2026.07.11.15.28"; requiring 0-255 per
// octet and a non-digit boundary keeps those intact.
var ipRe = regexp.MustCompile(
	`(^|[^0-9.])((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])([^0-9.]|$)`,
)

// versionContextRe marks lines whose dotted quad is a software version rather
// than an address. EE.log logs "PhysX Core Version: 4.1.1.0", which no pattern
// can tell apart from an address -- 4.1.1.0 is a syntactically valid one -- so
// the distinction has to come from context.
//
// This list is a deliberate exception, not a filter: anything not matched here
// gets redacted. A new version line in some future game build is redacted
// harmlessly until someone adds it; the failure mode is a mangled version
// string, never a leaked address.
var versionContextRe = regexp.MustCompile(`(?i)\b(version|build label|sdk)\b`)

const ipPlaceholder = "<ip>"

// redactLine removes IPv4 addresses from a log line before it leaves this
// machine.
//
// EE.log records the owner's public address (NAT binding, "public address")
// and, because matchmaking is peer-to-peer, squadmates' addresses too. Not all
// of them carry ports, so presence of a port cannot be used to identify them.
// None of it is needed to reconstruct what happened in a mission, so it is
// dropped at the producer rather than stored and dealt with later.
//
// Ports are kept: they carry useful connection detail and identify nobody.
func redactLine(s string) string {
	// Cheap reject first: an address needs at least three dots, and the regexes
	// are expensive enough that scanning for them on every line dominates the
	// operator's cost. Most log lines have no address and exit here.
	if !hasThreeDots(s) {
		return s
	}
	if versionContextRe.MatchString(s) {
		return s
	}
	return ipRe.ReplaceAllStringFunc(s, func(m string) string {
		// The pattern captures a boundary character on each side; preserve them.
		var lead, trail string
		if len(m) > 0 && !isIPByte(m[0]) {
			lead = string(m[0])
		}
		if n := len(m); n > 0 && !isIPByte(m[n-1]) {
			trail = string(m[n-1])
		}
		return lead + ipPlaceholder + trail
	})
}

func isIPByte(c byte) bool { return (c >= '0' && c <= '9') || c == '.' }

// hasThreeDots reports whether a line could possibly hold a dotted quad. It is
// a fast path, not a matcher: false means "definitely no address", true means
// "worth running the real patterns".
func hasThreeDots(s string) bool {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			if n++; n == 3 {
				return true
			}
		}
	}
	return false
}
