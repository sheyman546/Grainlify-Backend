// Package soroban – fuzz and property tests for XDR decoding helpers.
//
// # Security rationale
//
// The Decode* helpers in xdr_helpers.go consume data that ultimately originates
// from a Soroban RPC response.  A misbehaving or compromised RPC endpoint could
// return truncated, type-confused, or otherwise adversarial XDR.  Any panic in a
// decode call translates directly to a remote DoS against the API handler or
// worker goroutine that called it, so we treat every crash found here as a
// security bug.
//
// # Test structure
//
// 1. FuzzDecodeScVal* – one fuzz target per exported Decode function.
//    Each target feeds arbitrary bytes through xdr.SafeUnmarshal → Decode*.
//    The only invariant checked is "no panic"; errors are expected and fine.
//
// 2. FuzzDecodeScValRoundTrip – encodes a value with the matching Encode helper,
//    marshals it to XDR bytes, then drives the full unmarshal → decode pipeline.
//    Verifies that a round-trip on engine-produced bytes never panics and always
//    succeeds (no error).
//
// 3. Property table tests (TestDecodeNeverPanics*) – deterministic, fast,
//    no -fuzz flag required.  They cover truncated inputs, all-zero bytes,
//    single-byte sequences, and known-bad type tags so that the CI "go test"
//    run (without fuzzing) still exercises every panic-guarded path.
//
// # Running fuzz targets
//
//	# 30-second smoke run for all targets (matches CI step):
//	go test ./internal/soroban/... -run='^$' -fuzz=FuzzDecode -fuzztime=30s
//
//	# Longer targeted run:
//	go test ./internal/soroban/... -run='^$' -fuzz=FuzzDecodeScValStruct -fuzztime=5m
package soroban

import (
	"encoding/binary"
	"testing"

	"github.com/stellar/go/xdr"
)

// ── shared seed helpers ────────────────────────────────────────────────────────

// knownGoodScValInt64Bytes returns a minimal, well-formed XDR encoding of an
// ScvI64 ScVal so that the fuzzer starts from a valid corpus entry and mutates
// outwards, rather than spending all its time on random garbage that never
// reaches the type-switch in the decoder.
func knownGoodScValInt64Bytes() []byte {
	v := xdr.ScVal{
		Type: xdr.ScValTypeScvI64,
		I64:  ptr64(42),
	}
	b, _ := v.MarshalBinary()
	return b
}

func knownGoodScValStringBytes() []byte {
	s := xdr.ScString("hello")
	v := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &s}
	b, _ := v.MarshalBinary()
	return b
}

func knownGoodScValSymbolBytes() []byte {
	sym := xdr.ScSymbol("transfer")
	v := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	b, _ := v.MarshalBinary()
	return b
}

func knownGoodScValAddressBytes() []byte {
	// 32-byte all-zero contract hash → valid ScvAddress/contract variant.
	var id xdr.ContractId
	addr := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &id,
	}
	v := xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
	b, _ := v.MarshalBinary()
	return b
}

func knownGoodScValStructBytes() []byte {
	sym := xdr.ScSymbol("amount")
	i64 := xdr.Int64(100)
	val := xdr.ScVal{Type: xdr.ScValTypeScvI64, I64: &i64}
	m := xdr.ScMap{{
		Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
		Val: val,
	}}
	mp := &m
	v := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mp}
	b, _ := v.MarshalBinary()
	return b
}

// ptr64 is a helper to take the address of an xdr.Int64 literal.
func ptr64(i int64) *xdr.Int64 { v := xdr.Int64(i); return &v }

// decodeAndIgnoreError calls f and discards its return values – the only thing
// we care about is that it does not panic.
func mustNotPanic(f func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	f()
	return false
}

// ── Fuzz targets ──────────────────────────────────────────────────────────────

// FuzzDecodeScValInt64 feeds arbitrary bytes through the full RPC-response
// pipeline: raw bytes → xdr.SafeUnmarshal → DecodeScValInt64.
// Invariant: must never panic regardless of input.
func FuzzDecodeScValInt64(f *testing.F) {
	// Seed 1: well-formed ScvI64
	f.Add(knownGoodScValInt64Bytes())
	// Seed 2: well-formed ScvString (wrong type – decoder must return error)
	f.Add(knownGoodScValStringBytes())
	// Seed 3: empty input
	f.Add([]byte{})
	// Seed 4: all-zero bytes (4 bytes = minimum XDR envelope)
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	// Seed 5: single byte
	f.Add([]byte{0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			// Malformed XDR – unmarshal itself errored; nothing to decode.
			return
		}
		// After a successful unmarshal, Decode must not panic.
		if panicked := mustNotPanic(func() { DecodeScValInt64(v) }); panicked { //nolint:errcheck
			t.Fatalf("DecodeScValInt64 panicked on input: %x", data)
		}
	})
}

// FuzzDecodeScValString feeds arbitrary bytes into DecodeScValString.
func FuzzDecodeScValString(f *testing.F) {
	f.Add(knownGoodScValStringBytes())
	f.Add(knownGoodScValInt64Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0xde, 0xad, 0xbe, 0xef})

	f.Fuzz(func(t *testing.T, data []byte) {
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			return
		}
		if panicked := mustNotPanic(func() { DecodeScValString(v) }); panicked { //nolint:errcheck
			t.Fatalf("DecodeScValString panicked on input: %x", data)
		}
	})
}

// FuzzDecodeScValSymbol feeds arbitrary bytes into DecodeScValSymbol.
func FuzzDecodeScValSymbol(f *testing.F) {
	f.Add(knownGoodScValSymbolBytes())
	f.Add(knownGoodScValStringBytes())
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05})

	f.Fuzz(func(t *testing.T, data []byte) {
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			return
		}
		if panicked := mustNotPanic(func() { DecodeScValSymbol(v) }); panicked { //nolint:errcheck
			t.Fatalf("DecodeScValSymbol panicked on input: %x", data)
		}
	})
}

// FuzzDecodeScValAddress feeds arbitrary bytes into DecodeScValAddress.
func FuzzDecodeScValAddress(f *testing.F) {
	f.Add(knownGoodScValAddressBytes())
	f.Add(knownGoodScValInt64Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	// Type tag for ScvAddress with garbage payload
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], uint32(xdr.ScValTypeScvAddress))
	f.Add(buf)

	f.Fuzz(func(t *testing.T, data []byte) {
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			return
		}
		if panicked := mustNotPanic(func() { DecodeScValAddress(v) }); panicked { //nolint:errcheck
			t.Fatalf("DecodeScValAddress panicked on input: %x", data)
		}
	})
}

// FuzzDecodeScValStruct feeds arbitrary bytes into DecodeScValStruct.
// Maps have the richest internal structure and are most likely to have
// nil-pointer or out-of-bounds edge cases, so this target is especially
// valuable.
func FuzzDecodeScValStruct(f *testing.F) {
	f.Add(knownGoodScValStructBytes())
	f.Add(knownGoodScValStringBytes())
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	// Type tag for ScvMap with a claimed length of 0xffffffff – should be
	// rejected by SafeUnmarshal's allocation guard, but we seed it anyway.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], uint32(xdr.ScValTypeScvMap))
	binary.BigEndian.PutUint32(buf[4:8], 0xffffffff)
	f.Add(buf)

	f.Fuzz(func(t *testing.T, data []byte) {
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			return
		}
		if panicked := mustNotPanic(func() { DecodeScValStruct(v) }); panicked { //nolint:errcheck
			t.Fatalf("DecodeScValStruct panicked on input: %x", data)
		}
	})
}

// ── Round-trip fuzz ───────────────────────────────────────────────────────────

// FuzzDecodeScValRoundTrip verifies encode→marshal→unmarshal→decode never
// panics AND always succeeds (no error) for values produced by the Encode*
// helpers.  The fuzzer mutates the int64 seed value; the marshalled bytes are
// always structurally valid, so any unmarshal or decode error here is a bug.
func FuzzDecodeScValRoundTrip(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(1))
	f.Add(int64(-1))
	f.Add(int64(9223372036854775807))  // math.MaxInt64
	f.Add(int64(-9223372036854775808)) // math.MinInt64

	f.Fuzz(func(t *testing.T, n int64) {
		// Encode
		scVal, err := EncodeScValInt64(n)
		if err != nil {
			t.Fatalf("EncodeScValInt64(%d) returned unexpected error: %v", n, err)
		}

		// Marshal to XDR bytes
		raw, err := scVal.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary failed for %d: %v", n, err)
		}

		// Unmarshal
		var decoded xdr.ScVal
		if err := xdr.SafeUnmarshal(raw, &decoded); err != nil {
			t.Fatalf("SafeUnmarshal of engine-produced bytes failed for %d: %v", n, err)
		}

		// Decode – must not panic and must succeed
		var result int64
		if panicked := mustNotPanic(func() {
			result, err = DecodeScValInt64(decoded)
		}); panicked {
			t.Fatalf("DecodeScValInt64 panicked on round-trip for %d", n)
		}
		if err != nil {
			t.Fatalf("DecodeScValInt64 returned error on round-trip for %d: %v", n, err)
		}
		if result != n {
			t.Fatalf("round-trip value mismatch: encoded %d, decoded %d", n, result)
		}
	})
}

// ── Deterministic property table tests ───────────────────────────────────────
// These run under plain `go test` (no -fuzz flag) and cover the most important
// adversarial inputs so CI catches regressions even without a fuzz step.

// truncatedVariants returns a slice of increasingly-truncated prefixes of b,
// plus an empty slice and a single-byte slice.
func truncatedVariants(b []byte) [][]byte {
	out := [][]byte{{}, {0x00}}
	for cut := 1; cut < len(b); cut++ {
		cp := make([]byte, cut)
		copy(cp, b[:cut])
		out = append(out, cp)
	}
	return out
}

// allZeroVariants returns zero-filled byte slices of common lengths.
func allZeroVariants() [][]byte {
	lens := []int{1, 2, 3, 4, 7, 8, 15, 16, 32, 64, 128}
	out := make([][]byte, len(lens))
	for i, l := range lens {
		out[i] = make([]byte, l)
	}
	return out
}

// TestDecodeNeverPanics_Int64 exercises truncated, zero, and type-mismatch
// inputs for DecodeScValInt64 without needing the -fuzz flag.
func TestDecodeNeverPanics_Int64(t *testing.T) {
	candidates := append(truncatedVariants(knownGoodScValInt64Bytes()), allZeroVariants()...)
	// Also add a well-formed ScvString so we test type-mismatch path.
	candidates = append(candidates, knownGoodScValStringBytes())

	for _, data := range candidates {
		data := data // capture
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			continue // malformed XDR; not our concern
		}
		if panicked := mustNotPanic(func() { DecodeScValInt64(v) }); panicked { //nolint:errcheck
			t.Errorf("DecodeScValInt64 panicked on input %x", data)
		}
	}
}

// TestDecodeNeverPanics_String exercises DecodeScValString.
func TestDecodeNeverPanics_String(t *testing.T) {
	candidates := append(truncatedVariants(knownGoodScValStringBytes()), allZeroVariants()...)
	candidates = append(candidates, knownGoodScValSymbolBytes())

	for _, data := range candidates {
		data := data
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			continue
		}
		if panicked := mustNotPanic(func() { DecodeScValString(v) }); panicked { //nolint:errcheck
			t.Errorf("DecodeScValString panicked on input %x", data)
		}
	}
}

// TestDecodeNeverPanics_Symbol exercises DecodeScValSymbol.
func TestDecodeNeverPanics_Symbol(t *testing.T) {
	candidates := append(truncatedVariants(knownGoodScValSymbolBytes()), allZeroVariants()...)
	candidates = append(candidates, knownGoodScValInt64Bytes())

	for _, data := range candidates {
		data := data
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			continue
		}
		if panicked := mustNotPanic(func() { DecodeScValSymbol(v) }); panicked { //nolint:errcheck
			t.Errorf("DecodeScValSymbol panicked on input %x", data)
		}
	}
}

// TestDecodeNeverPanics_Address exercises DecodeScValAddress including the
// unknown-address-type branch which is easy to miss with hand-written tests.
func TestDecodeNeverPanics_Address(t *testing.T) {
	candidates := append(truncatedVariants(knownGoodScValAddressBytes()), allZeroVariants()...)
	// Inject an unknown address type (ScAddressType=99) inside a valid ScvAddress envelope.
	unknownAddrType := xdr.ScAddress{Type: xdr.ScAddressType(99)}
	v := xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &unknownAddrType}
	if raw, err := v.MarshalBinary(); err == nil {
		candidates = append(candidates, raw)
	}

	for _, data := range candidates {
		data := data
		var sc xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &sc); err != nil {
			continue
		}
		if panicked := mustNotPanic(func() { DecodeScValAddress(sc) }); panicked { //nolint:errcheck
			t.Errorf("DecodeScValAddress panicked on input %x", data)
		}
	}
}

// TestDecodeNeverPanics_Struct is the most thorough property test because
// ScvMap has the most complex internal structure.
func TestDecodeNeverPanics_Struct(t *testing.T) {
	candidates := append(truncatedVariants(knownGoodScValStructBytes()), allZeroVariants()...)
	// Oversized claimed map length – SafeUnmarshal should reject this, but
	// if it ever gets through the decoder must still not panic.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], uint32(xdr.ScValTypeScvMap))
	binary.BigEndian.PutUint32(buf[4:8], 0x00ffffff)
	candidates = append(candidates, buf)

	for _, data := range candidates {
		data := data
		var v xdr.ScVal
		if err := xdr.SafeUnmarshal(data, &v); err != nil {
			continue
		}
		if panicked := mustNotPanic(func() { DecodeScValStruct(v) }); panicked { //nolint:errcheck
			t.Errorf("DecodeScValStruct panicked on input %x", data)
		}
	}
}

// TestDecodeRoundTrip_AllTypes verifies encode→decode consistency for every
// Encode/Decode pair using deterministic inputs.
func TestDecodeRoundTrip_AllTypes(t *testing.T) {
	t.Run("Int64", func(t *testing.T) {
		for _, n := range []int64{0, 1, -1, 1<<62 - 1, -(1 << 62)} {
			enc, err := EncodeScValInt64(n)
			if err != nil {
				t.Fatalf("encode %d: %v", n, err)
			}
			got, err := DecodeScValInt64(enc)
			if err != nil {
				t.Fatalf("decode %d: %v", n, err)
			}
			if got != n {
				t.Errorf("round-trip mismatch: want %d got %d", n, got)
			}
		}
	})

	t.Run("String", func(t *testing.T) {
		for _, s := range []string{"", "hello", "日本語", string(make([]byte, 1024))} {
			enc, err := EncodeScValString(s)
			if err != nil {
				t.Fatalf("encode %q: %v", s, err)
			}
			got, err := DecodeScValString(enc)
			if err != nil {
				t.Fatalf("decode %q: %v", s, err)
			}
			if got != s {
				t.Errorf("round-trip mismatch: want %q got %q", s, got)
			}
		}
	})
}
