package main

import (
	"bytes"
	"context"
	"math/big"
	"net"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/signerpb"
)

// stubSignerServer is a hermetic SignerServiceServer that captures the request it
// receives and returns either a canned response or a chosen status error. It lets
// the encode/decode surface be asserted without a real signer or key material.
type stubSignerServer struct {
	signerpb.UnimplementedSignerServiceServer

	got  *signerpb.SignTransactionRequest
	resp *signerpb.SignTransactionResponse
	err  error
}

func (s *stubSignerServer) SignTransaction(_ context.Context, req *signerpb.SignTransactionRequest) (*signerpb.SignTransactionResponse, error) {
	s.got = req
	if s.err != nil {
		return nil, s.err
	}
	return s.resp, nil
}

// dialStub stands the stub up over an in-memory bufconn and returns the real
// production signerClient dialing it. Fully hermetic — no network, no signer.
func dialStub(t *testing.T, stub *stubSignerServer) *signerClient {
	t.Helper()

	srv := grpc.NewServer()
	signerpb.RegisterSignerServiceServer(srv, stub)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return newSignerClient(signerpb.NewSignerServiceClient(conn))
}

func TestSignerClient_EncodesRequestOntoWire(t *testing.T) {
	// A request with distinct, multi-byte uint256 values so the big-endian
	// round-trip is a real assertion, not a 0/1 coincidence.
	to := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
	req := evm.SignerRequest{
		KeyID:                "hot",
		ChainID:              11155111,
		Nonce:                42,
		GasLimit:             65_000,
		To:                   to,
		Value:                big.NewInt(0),                                         // ERC-20 transfer moves no ETH
		MaxFeePerGas:         new(big.Int).SetBytes([]byte{0x01, 0x00, 0x00, 0x00}), // 2^24
		MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		Data:                 []byte{0xa9, 0x05, 0x9c, 0xbb, 0xde, 0xad},
	}

	stub := &stubSignerServer{
		resp: &signerpb.SignTransactionResponse{
			RawTransaction: []byte{0x02, 0xf8, 0x01},
			TxHash:         common.HexToHash("0xabc123").Bytes(),
			From:           to.Hex(),
		},
	}
	client := dialStub(t, stub)

	if _, err := client.Sign(context.Background(), req); err != nil {
		t.Fatalf("Sign() = %v, want nil", err)
	}

	got := stub.got
	// Destination: exactly 20 bytes, equal to req.To.Bytes().
	if len(got.GetTo()) != 20 {
		t.Fatalf("wire To length = %d, want 20", len(got.GetTo()))
	}
	if !bytes.Equal(got.GetTo(), to.Bytes()) {
		t.Fatalf("wire To = %x, want %x", got.GetTo(), to.Bytes())
	}
	// uint256 fields must decode back to the exact input via SetBytes (proves the
	// minimal big-endian round-trip is the signer boundary's exact inverse).
	assertUint256(t, "value", got.GetValue(), req.Value)
	assertUint256(t, "max_fee_per_gas", got.GetMaxFeePerGas(), req.MaxFeePerGas)
	assertUint256(t, "max_priority_fee_per_gas", got.GetMaxPriorityFeePerGas(), req.MaxPriorityFeePerGas)
	// Scalar/opaque fields cross straight.
	if got.GetKeyId() != req.KeyID {
		t.Errorf("wire KeyId = %q, want %q", got.GetKeyId(), req.KeyID)
	}
	if got.GetChainId() != req.ChainID {
		t.Errorf("wire ChainId = %d, want %d", got.GetChainId(), req.ChainID)
	}
	if got.GetNonce() != req.Nonce {
		t.Errorf("wire Nonce = %d, want %d", got.GetNonce(), req.Nonce)
	}
	if got.GetGasLimit() != req.GasLimit {
		t.Errorf("wire GasLimit = %d, want %d", got.GetGasLimit(), req.GasLimit)
	}
	if !bytes.Equal(got.GetData(), req.Data) {
		t.Errorf("wire Data = %x, want %x", got.GetData(), req.Data)
	}
}

func TestSignerClient_DecodesResponse(t *testing.T) {
	raw := []byte{0x02, 0xf8, 0x99}
	txHash := common.HexToHash("0x1234")
	from := common.HexToAddress("0x00000000000000000000000000000000000000Fe")

	stub := &stubSignerServer{
		resp: &signerpb.SignTransactionResponse{
			RawTransaction: raw,
			TxHash:         txHash.Bytes(),
			From:           from.Hex(),
		},
	}
	client := dialStub(t, stub)

	signed, err := client.Sign(context.Background(), validSignerRequest())
	if err != nil {
		t.Fatalf("Sign() = %v, want nil", err)
	}
	if !bytes.Equal(signed.RawTransaction, raw) {
		t.Errorf("RawTransaction = %x, want %x", signed.RawTransaction, raw)
	}
	if signed.TxHash != txHash {
		t.Errorf("TxHash = %s, want %s", signed.TxHash, txHash)
	}
	if signed.From != from {
		t.Errorf("From = %s, want %s", signed.From, from)
	}
}

func TestSignerClient_PreservesStatusCode(t *testing.T) {
	// Each gRPC code the signer can return must survive as the wrapped error's
	// status.Code — the client must not flatten it to Unknown or Internal.
	for _, code := range []codes.Code{codes.NotFound, codes.InvalidArgument, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			stub := &stubSignerServer{err: status.Error(code, "boom")}
			client := dialStub(t, stub)

			_, err := client.Sign(context.Background(), validSignerRequest())
			if err == nil {
				t.Fatalf("Sign() = nil error, want code %s", code)
			}
			if got := status.Code(err); got != code {
				t.Fatalf("status.Code(err) = %s, want %s", got, code)
			}
		})
	}
}

// validSignerRequest is a well-formed request the error/decode tests reuse.
func validSignerRequest() evm.SignerRequest {
	return evm.SignerRequest{
		KeyID:                "hot",
		ChainID:              1,
		Nonce:                1,
		GasLimit:             65_000,
		To:                   common.HexToAddress("0x000000000000000000000000000000000000dEaD"),
		Value:                big.NewInt(0),
		MaxFeePerGas:         big.NewInt(50_000_000_000),
		MaxPriorityFeePerGas: big.NewInt(1_000_000_000),
		Data:                 []byte{0xa9, 0x05, 0x9c, 0xbb},
	}
}

// assertUint256 decodes a wire field with SetBytes (the signer boundary's exact
// decode) and asserts it equals the original *big.Int.
func assertUint256(t *testing.T, field string, wire []byte, want *big.Int) {
	t.Helper()
	if got := new(big.Int).SetBytes(wire); got.Cmp(want) != 0 {
		t.Errorf("wire %s decodes to %s, want %s (bytes %x)", field, got, want, wire)
	}
}
