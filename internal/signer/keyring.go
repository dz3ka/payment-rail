package signer

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// keyEntry is one loaded signing key together with the policy state bound to it.
//
// privateKey is a pointer (an *ecdsa.PrivateKey) because that is what
// go-ethereum's crypto package hands back and consumes — and because the key is
// unique, shared, mutable-in-principle material we never want to copy by value.
// It is unexported and must never be logged, formatted with %v, or embedded in
// an error: nothing outside this package can reach it, and nothing inside it may
// leak it. address, by contrast, is a value type (a 20-byte array) safe to copy
// and expose freely.
//
// bucket is the per-key spend limiter. Colocating it with the key makes the key
// itself the serialization point (see spendBucket): a lookup returns both the
// signing material and the lock that guards its budget.
type keyEntry struct {
	privateKey *ecdsa.PrivateKey
	address    common.Address
	chainID    uint64
	bucket     *spendBucket
}

// Keyring is the immutable set of keys the signer will sign with, indexed by
// key_id. It is populated once by LoadKeyring and only read afterwards, so
// concurrent lookups need no locking; the per-key mutation (spend counting)
// lives behind each key's own bucket mutex, not on the ring.
type Keyring struct {
	keys map[string]*keyEntry
}

// manifestEntry mirrors one element of the committed, secret-free key manifest.
// The manifest holds no key material — only a pointer to a key file, the chain
// the key is bound to, and its spend ceiling.
type manifestEntry struct {
	KeyID   string `json:"key_id"`
	KeyFile string `json:"key_file"`
	ChainID uint64 `json:"chain_id"`
	// SpendLimit is a decimal wei string (a uint256 does not fit in any Go
	// integer), parsed to *big.Int at load.
	SpendLimit string `json:"spend_limit"`
}

// LoadKeyring reads a manifest and builds the in-memory keyring. key_file paths
// are resolved relative to the manifest's own directory. It fails closed: a
// duplicate key_id, an unparseable spend_limit, a missing or over-permissioned
// key file, or malformed key hex all abort the whole load rather than silently
// dropping a key.
func LoadKeyring(manifestPath string) (*Keyring, error) {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("signer: read manifest: %w", err)
	}
	var entries []manifestEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("signer: parse manifest: %w", err)
	}

	dir := filepath.Dir(manifestPath)
	keys := make(map[string]*keyEntry, len(entries))
	for _, e := range entries {
		if e.KeyID == "" {
			return nil, fmt.Errorf("signer: manifest entry is missing key_id")
		}
		if _, dup := keys[e.KeyID]; dup {
			return nil, fmt.Errorf("signer: duplicate key_id %q in manifest", e.KeyID)
		}
		// SetString(_, 10) rejects non-decimal input; the sign check rejects a
		// negative ceiling. The limit value itself is not echoed on failure.
		limit, ok := new(big.Int).SetString(e.SpendLimit, 10)
		if !ok || limit.Sign() < 0 {
			return nil, fmt.Errorf("signer: key_id %q has an invalid spend_limit", e.KeyID)
		}
		priv, addr, err := loadKeyFile(dir, e.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("signer: key_id %q: %w", e.KeyID, err)
		}
		keys[e.KeyID] = &keyEntry{
			privateKey: priv,
			address:    addr,
			chainID:    e.ChainID,
			bucket:     newSpendBucket(limit),
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("signer: manifest contains no keys")
	}
	return &Keyring{keys: keys}, nil
}

// loadKeyFile reads a hex private key file and derives its address. It refuses
// any file readable by group or world, parses the hex via go-ethereum, and
// never lets key bytes reach an error message.
func loadKeyFile(dir, keyFile string) (*ecdsa.PrivateKey, common.Address, error) {
	if keyFile == "" {
		return nil, common.Address{}, fmt.Errorf("key_file is required")
	}
	path := keyFile
	if !filepath.IsAbs(path) {
		path = filepath.Join(dir, keyFile)
	}

	// Permission gate: a private key file that others can read is treated as
	// compromised. mode&0o077 catches any group- or world- read/write/execute
	// bit; only owner-accessible files (e.g. 0600, 0400) pass. This is the last
	// cheap defense before secret material enters the process.
	info, err := os.Stat(path)
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("stat key_file: %w", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, common.Address{}, fmt.Errorf("key_file %s is group/world-accessible (mode %#o); refusing to load", path, perm)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("read key_file: %w", err)
	}
	hexKey := strings.TrimPrefix(strings.TrimSpace(string(raw)), "0x")
	priv, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		// Deliberately do NOT wrap err: go-ethereum's parse errors can include
		// fragments of the input, and the input is the private key. Report a
		// fixed message so no key material ever reaches a log or caller.
		return nil, common.Address{}, fmt.Errorf("key_file %s does not contain a valid hex private key", path)
	}
	// PubkeyToAddress derives the 20-byte address deterministically from the
	// public half — no secret is exposed by returning it.
	return priv, crypto.PubkeyToAddress(priv.PublicKey), nil
}

// lookup returns the key registered for keyID. The bool is false for an unknown
// key_id, which policy translates into ErrUnknownKey.
func (kr *Keyring) lookup(keyID string) (*keyEntry, bool) {
	k, ok := kr.keys[keyID]
	return k, ok
}
