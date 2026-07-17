package main

import (
	"context"
	"errors"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dz3ka/payment-rail/internal/signer"
	"github.com/dz3ka/payment-rail/internal/signerpb"
)

// Server is the gRPC adapter for the signing domain. It is the trust boundary
// between the wire (raw bytes for addresses and uint256 amounts) and the domain
// (common.Address / *big.Int): it validates lengths BEFORE converting, calls
// signer.Sign, and maps the domain's sentinel errors onto gRPC status codes. It
// holds no key material and knows nothing about how signing works — the domain
// owns that.
type Server struct {
	// Embedding the generated UnimplementedSignerServiceServer (by value, per the
	// generated code's contract) keeps this forward-compatible if the service
	// grows methods this adapter has not yet implemented.
	signerpb.UnimplementedSignerServiceServer

	signer *signer.Signer
	log    *slog.Logger
}

// NewServer wraps a *signer.Signer in the gRPC adapter. A nil logger falls back
// to slog.Default() so callers that do not care about logging need not build one
// (mirrors ledger.NewService).
func NewServer(s *signer.Signer, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{signer: s, log: log}
}

// SignTransaction converts the request at the trust boundary, signs it, and maps
// the outcome back to the wire. Every request produces exactly one structured
// outcome log line carrying the gRPC code and the (non-secret) key_id — never an
// amount, limit, sender, or raw transaction.
func (s *Server) SignTransaction(ctx context.Context, req *signerpb.SignTransactionRequest) (*signerpb.SignTransactionResponse, error) {
	// Boundary validation + conversion happens before we touch the domain: a
	// length-invalid input never reaches signer.Sign as a silently padded value.
	domReq, err := toDomainRequest(req)
	if err != nil {
		s.logResult(ctx, req.GetKeyId(), status.Code(err))
		return nil, err
	}

	signed, err := s.signer.Sign(ctx, domReq)
	if err != nil {
		gerr := mapSignError(err)
		s.logResult(ctx, req.GetKeyId(), status.Code(gerr))
		return nil, gerr
	}

	s.logResult(ctx, req.GetKeyId(), codes.OK)
	return &signerpb.SignTransactionResponse{
		RawTransaction: signed.RawTransaction,
		TxHash:         signed.TxHash.Bytes(),
		From:           signed.From.Hex(), // 0x-prefixed, EIP-55 checksummed
	}, nil
}

// toDomainRequest maps the proto message onto a signer.SignRequest, validating
// the byte-length of every field whose conversion would otherwise lose length
// information. It is the whole reason this adapter exists: common.BytesToAddress
// silently pads/truncates, and big.Int.SetBytes silently accepts oversized
// input, so a caller could smuggle a malformed value past the domain unless the
// lengths are checked here first. Errors are gRPC InvalidArgument with a generic
// message — the field name is safe to name, the value is not.
func toDomainRequest(req *signerpb.SignTransactionRequest) (signer.SignRequest, error) {
	// A wrong-length destination must be rejected, not padded/truncated into a
	// different-but-valid address. The domain re-checks for the zero address
	// (contract creation); here we only guarantee the 20-byte shape.
	if len(req.GetTo()) != 20 {
		return signer.SignRequest{}, status.Error(codes.InvalidArgument, "malformed transaction: destination address must be 20 bytes")
	}

	value, err := toUint256(req.GetValue(), "value")
	if err != nil {
		return signer.SignRequest{}, err
	}
	maxFee, err := toUint256(req.GetMaxFeePerGas(), "max_fee_per_gas")
	if err != nil {
		return signer.SignRequest{}, err
	}
	maxTip, err := toUint256(req.GetMaxPriorityFeePerGas(), "max_priority_fee_per_gas")
	if err != nil {
		return signer.SignRequest{}, err
	}

	return signer.SignRequest{
		KeyID:                req.GetKeyId(),
		ChainID:              req.GetChainId(),
		Nonce:                req.GetNonce(),
		GasLimit:             req.GetGasLimit(),
		To:                   common.BytesToAddress(req.GetTo()),
		Value:                value,
		MaxFeePerGas:         maxFee,
		MaxPriorityFeePerGas: maxTip,
		Data:                 req.GetData(),
	}, nil
}

// toUint256 decodes a big-endian uint256 field. A field longer than 32 bytes
// cannot be a uint256 and is rejected here rather than silently accepted by
// SetBytes. An empty/nil field decodes to zero: SetBytes always returns a
// non-nil *big.Int, so the domain never sees a nil pointer.
func toUint256(b []byte, field string) (*big.Int, error) {
	if len(b) > 32 {
		return nil, status.Errorf(codes.InvalidArgument, "malformed transaction: %s must be at most 32 bytes", field)
	}
	return new(big.Int).SetBytes(b), nil
}

// mapSignError translates a domain sentinel into a gRPC status. Messages carry
// the coarse reason only — never key material, amounts, limits, or a sender
// derived from a key. Anything outside the known sentinels is an internal fault.
func mapSignError(err error) error {
	switch {
	case errors.Is(err, signer.ErrUnknownKey):
		return status.Error(codes.NotFound, "unknown key")
	case errors.Is(err, signer.ErrChainMismatch):
		return status.Error(codes.InvalidArgument, "chain_id does not match key")
	case errors.Is(err, signer.ErrMalformedTx):
		return status.Error(codes.InvalidArgument, "malformed transaction")
	case errors.Is(err, signer.ErrSpendLimitExceeded):
		return status.Error(codes.ResourceExhausted, "spend limit exceeded")
	default:
		return status.Error(codes.Internal, "internal signing error")
	}
}

// logResult emits one structured record per request outcome, mirroring ledger's
// logResult discipline: the code and the opaque key_id are logged, but amounts,
// limits, the recovered sender, and the raw transaction never are. Expected
// client-side rejections are info/warn; an internal fault is error.
func (s *Server) logResult(ctx context.Context, keyID string, code codes.Code) {
	attrs := []any{"key_id", keyID, "code", code.String()}
	switch code {
	case codes.OK:
		s.log.InfoContext(ctx, "transaction signed", attrs...)
	case codes.NotFound:
		s.log.InfoContext(ctx, "sign rejected: unknown key", attrs...)
	case codes.InvalidArgument:
		s.log.InfoContext(ctx, "sign rejected: malformed request", attrs...)
	case codes.ResourceExhausted:
		s.log.WarnContext(ctx, "sign rejected: spend limit exceeded", attrs...)
	default:
		s.log.ErrorContext(ctx, "sign failed", attrs...)
	}
}
