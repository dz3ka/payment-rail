package main

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/grpc/status"

	"github.com/dz3ka/payment-rail/internal/chain/evm"
	"github.com/dz3ka/payment-rail/internal/signerpb"
)

// signerClient is the production evm.Signer: it maps the adapter's proto-free
// SignerRequest onto the signerpb wire and the response back, so the adapter
// (internal/chain/evm) stays free of the generated gRPC types. This is the one
// place in the composition root that speaks both dialects.
//
// The uint256 fields (value, fee caps) cross the wire as big-endian bytes.
// (*big.Int).Bytes() is minimal big-endian and the exact inverse of the signer
// boundary's new(big.Int).SetBytes (see cmd/signer/server.go toDomainRequest),
// so a value round-trips octet-for-octet — no decimal string to disagree on, no
// precision loss. A zero value encodes as an empty slice, which SetBytes reads
// back as 0.
type signerClient struct {
	rpc signerpb.SignerServiceClient
}

// newSignerClient wraps a generated gRPC client in the evm.Signer port.
func newSignerClient(rpc signerpb.SignerServiceClient) *signerClient {
	return &signerClient{rpc: rpc}
}

// Compile-time proof the client satisfies the port the adapter signs through.
var _ evm.Signer = (*signerClient)(nil)

// Sign encodes the domain request onto the wire, calls the isolated signer, and
// decodes the signed transaction back.
//
// It does NOT apply a chain.* sentinel: the adapter owns that mapping (it wraps
// any Sign error as chain.ErrSignerRejected). On a gRPC failure we preserve the
// status code in the wrapped error so an operator or log can still tell a
// NotFound (unknown key) from a ResourceExhausted (spend limit) even though the
// adapter collapses both to "signer declined".
func (s *signerClient) Sign(ctx context.Context, req evm.SignerRequest) (evm.SignedTx, error) {
	resp, err := s.rpc.SignTransaction(ctx, &signerpb.SignTransactionRequest{
		KeyId:                req.KeyID,
		ChainId:              req.ChainID,
		Nonce:                req.Nonce,
		GasLimit:             req.GasLimit,
		To:                   req.To.Bytes(),                   // 20 bytes
		Value:                req.Value.Bytes(),                // minimal big-endian
		MaxFeePerGas:         req.MaxFeePerGas.Bytes(),         // minimal big-endian
		MaxPriorityFeePerGas: req.MaxPriorityFeePerGas.Bytes(), // minimal big-endian
		Data:                 req.Data,
	})
	if err != nil {
		return evm.SignedTx{}, fmt.Errorf("signer rpc %s: %w", status.Code(err), err)
	}

	return evm.SignedTx{
		RawTransaction: resp.GetRawTransaction(),
		TxHash:         common.BytesToHash(resp.GetTxHash()),
		From:           common.HexToAddress(resp.GetFrom()),
	}, nil
}
