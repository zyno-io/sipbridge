package engine

import "testing"

func TestNativeIdentityPreservesMemoryFormat(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	if got := nativeIdentity(key, "ab", "c"); got != "a1a22d593090cd65b0e41a55eeed5d1f2ab13a7357116c4e299eb8740d495cb6" {
		t.Fatal("native identity changed", got)
	}
	if nativeIdentity(key, "ab", "c") == nativeIdentity(key, "a", "bc") {
		t.Fatal("identity tuple is ambiguous")
	}
}
