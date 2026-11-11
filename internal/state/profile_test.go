package state

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"sipbridge/internal/storagecodec"
)

func TestEachRecordRejectsIncompatibleConfiguration(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	ref := group(t, s, "profile-call")
	step := begin(t, s, ref, "initial", true)
	if _, err := s.Commit(ctx, *step, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"reducer": func(c *Config) { c.Versions.Reducer = "incompatible" },
		"timers":  func(c *Config) { c.Versions.Timers = "incompatible" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := s.cfg
			change(&cfg)
			other, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if _, err := other.Load(ctx, step.Record.Ref); !errors.Is(err, ErrAuthority) && !errors.Is(err, ErrVersion) {
				t.Fatal("incompatible record accepted", err)
			}
			if _, _, err := other.ReadState(ctx, step.Record.Ref); !errors.Is(err, ErrAuthority) && !errors.Is(err, ErrVersion) {
				t.Fatal("incompatible state view accepted", err)
			}
		})
	}
	if err := s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBinaryEncodingFencesJSONAuthorityWithoutChangingLogicalProfile(t *testing.T) {
	cfg := Config{Prefix: "sipstate:v1", Namespace: "binary-fence", NamespaceKey: bytes.Repeat([]byte{1}, 32), Versions: Versions{Schema: "1", Reducer: "1", Timers: "1"}}
	if err := commonDefaults(&cfg); err != nil {
		t.Fatal(err)
	}
	current := newMetadata(cfg)
	versions, _ := json.Marshal(cfg.Versions)
	limits, _ := json.Marshal(cfg.Limits)
	old := hmac.New(sha256.New, cfg.NamespaceKey)
	fmt.Fprintf(old, "sip-state-profile-v1:%s:%d:%s", current.root, cfg.Partitions, versions)
	old.Write(limits)
	oldProfile := hex.EncodeToString(old.Sum(nil))
	logical := hmac.New(sha256.New, cfg.NamespaceKey)
	fmt.Fprintf(logical, "sip-state-logical-authority-v1:%s:%s:%s", current.root, cfg.Generation, oldProfile)
	oldAuthority := hex.EncodeToString(logical.Sum(nil))[:40]
	if current.profile == oldProfile || current.authority.RunID == oldAuthority || current.cfg.Versions != (Versions{Schema: "1", Reducer: "1", Timers: "1"}) {
		t.Fatal("encoding did not independently fence authority")
	}
	preceding := hmac.New(sha256.New, cfg.NamespaceKey)
	fmt.Fprintf(preceding, "sip-state-profile-v1:%s:%d:%s:%s", current.root, cfg.Partitions, versions, storagecodec.Encoding)
	preceding.Write(limits)
	if current.profile == hex.EncodeToString(preceding.Sum(nil)) {
		t.Fatal("storage label alone admitted the preceding incompatible binary descriptor")
	}
}
