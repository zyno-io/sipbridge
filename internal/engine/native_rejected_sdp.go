package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"
)

// nativeRejectedSDP projects only what an all-rejected answer needs. It does
// not negotiate codecs or media, and retains no parsed SDP after construction.
func nativeRejectedSDP(offer []byte, branch string) []byte {
	identity := sha256.Sum256([]byte(branch))
	origin := strconv.FormatUint(binary.BigEndian.Uint64(identity[:8]), 10)
	header := "v=0\r\no=sipbridge " + origin + " 0 IN IP4 0.0.0.0\r\ns=-\r\nc=IN IP4 0.0.0.0\r\n"
	var timing, media strings.Builder
	malformed := false
	for line := range strings.Lines(string(offer)) {
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if strings.HasPrefix(line, "t=") {
			fields := strings.Fields(line[2:])
			if len(fields) != 2 {
				malformed = true
				break
			}
			for _, value := range fields {
				if _, err := strconv.ParseUint(value, 10, 64); err != nil {
					malformed = true
				}
			}
			timing.WriteString("t=" + strings.Join(fields, " ") + "\r\n")
		} else if strings.HasPrefix(line, "m=") {
			fields := strings.Fields(line[2:])
			if len(fields) < 4 || !nativeSDPToken(fields[0]) || !nativeSDPToken(fields[3]) {
				malformed = true
				break
			}
			for part := range strings.SplitSeq(fields[2], "/") {
				if !nativeSDPToken(part) {
					malformed = true
				}
			}
			media.WriteString("m=" + fields[0] + " 0 " + fields[2] + " " + fields[3] + "\r\n")
		}
	}
	if malformed || timing.Len() == 0 {
		return []byte(header + "t=0 0\r\na=inactive\r\nm=audio 0 RTP/AVP 0\r\n")
	}
	return []byte(header + timing.String() + "a=inactive\r\n" + media.String())
}

func nativeSDPToken(value string) bool {
	if value == "" {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if !(c == '!' || c >= '#' && c <= '\'' || c == '*' || c == '+' || c == '-' || c == '.' || c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= '^' && c <= '~') {
			return false
		}
	}
	return true
}
