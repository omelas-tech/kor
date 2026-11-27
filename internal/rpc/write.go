package rpc

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Write implements the streaming write RPC the native mobile SDKs use instead
// of Commit. The first request is a handshake with no writes; the response
// carries a stream id and token. Each later request is one atomic batch,
// answered with its write results, commit time and a fresh token.
//
// The tokens only acknowledge position: Kor commits each batch before it
// answers, so there is nothing for a token to resume, and an empty request
// (the SDK's way of acking a token) is answered with a new token alone.
func (s *Server) Write(stream pb.Firestore_WriteServer) error {
	ctx := stream.Context()
	guard, err := s.writeGuard(ctx)
	if err != nil {
		return err
	}
	var expires time.Time
	if s.sec != nil {
		_, expires, _ = s.sec.identity(ctx)
	}
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	if len(first.GetWrites()) > 0 {
		return status.Error(codes.InvalidArgument, "kor: the first write-stream request must be a handshake without writes")
	}
	var idb [8]byte
	if _, err := rand.Read(idb[:]); err != nil {
		return status.Errorf(codes.Internal, "kor: write stream id: %v", err)
	}
	id := hex.EncodeToString(idb[:])
	var seq uint64
	token := func() []byte { seq++; return []byte(fmt.Sprintf("%s:%d", id, seq)) }
	if err := stream.Send(&pb.WriteResponse{StreamId: id, StreamToken: token()}); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		resp := &pb.WriteResponse{StreamId: id}
		if !expires.IsZero() && time.Now().After(expires) {
			return status.Error(codes.Unauthenticated, "ID token expired; reconnect with a fresh one")
		}
		if ws := req.GetWrites(); len(ws) > 0 {
			results, commitTime, err := s.store.CommitGuarded(ctx, ws, guard)
			if err != nil {
				// The stream dies with the batch's error, as Firestore's does:
				// the SDK rejects that batch and reopens for the next.
				return err
			}
			resp.WriteResults, resp.CommitTime = results, timestamppb.New(commitTime)
		} else {
			resp.CommitTime = timestamppb.New(time.Now().UTC().Truncate(time.Microsecond))
		}
		resp.StreamToken = token()
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
