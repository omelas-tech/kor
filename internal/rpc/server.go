// Package rpc implements the google.firestore.v1.Firestore gRPC service on
// top of Kor's store. Firestore SDKs connect to it via
// FIRESTORE_EMULATOR_HOST or a custom host setting — no application changes.
package rpc

import (
	"context"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omelas-tech/kor/internal/store"
)

// Server implements pb.FirestoreServer.
type Server struct {
	pb.UnimplementedFirestoreServer
	store *store.Store
}

// New returns a Firestore service backed by st.
func New(st *store.Store) *Server {
	return &Server{store: st}
}

// Register attaches the service to a gRPC server.
func (s *Server) Register(g *grpc.Server) {
	pb.RegisterFirestoreServer(g, s)
}

// Commit applies writes atomically.
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	if req.GetTransaction() != nil {
		return nil, status.Error(codes.Unimplemented, "kor: transactions not implemented yet")
	}
	results, commitTime, err := s.store.Commit(ctx, req.GetWrites())
	if err != nil {
		return nil, err
	}
	return &pb.CommitResponse{
		WriteResults: results,
		CommitTime:   timestamppb.New(commitTime),
	}, nil
}

// BatchWrite applies writes non-atomically, returning a status per write.
// This is what BulkWriter uses; partial failure is expected and reported
// per-entry rather than failing the RPC.
func (s *Server) BatchWrite(ctx context.Context, req *pb.BatchWriteRequest) (*pb.BatchWriteResponse, error) {
	resp := &pb.BatchWriteResponse{}
	for _, w := range req.GetWrites() {
		results, _, err := s.store.Commit(ctx, []*pb.Write{w})
		if err != nil {
			resp.WriteResults = append(resp.WriteResults, &pb.WriteResult{})
			resp.Status = append(resp.Status, statusProto(err))
			continue
		}
		resp.WriteResults = append(resp.WriteResults, results[0])
		resp.Status = append(resp.Status, statusProto(nil))
	}
	return resp, nil
}

// GetDocument returns a single document (REST-oriented; server SDKs use
// BatchGetDocuments).
func (s *Server) GetDocument(ctx context.Context, req *pb.GetDocumentRequest) (*pb.Document, error) {
	if req.GetTransaction() != nil || req.GetReadTime() != nil {
		return nil, status.Error(codes.Unimplemented, "kor: read consistency selectors not implemented yet")
	}
	docs, err := s.store.GetDocuments(ctx, []string{req.GetName()})
	if err != nil {
		return nil, err
	}
	doc, ok := docs[req.GetName()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "document not found: %s", req.GetName())
	}
	return docProto(doc)
}

// BatchGetDocuments streams found/missing results for the requested names.
func (s *Server) BatchGetDocuments(req *pb.BatchGetDocumentsRequest, stream pb.Firestore_BatchGetDocumentsServer) error {
	if req.GetTransaction() != nil || req.GetNewTransaction() != nil || req.GetReadTime() != nil {
		return status.Error(codes.Unimplemented, "kor: read consistency selectors not implemented yet")
	}
	docs, err := s.store.GetDocuments(stream.Context(), req.GetDocuments())
	if err != nil {
		return err
	}
	readTime := timestamppb.New(time.Now().UTC().Truncate(time.Microsecond))
	for _, name := range req.GetDocuments() {
		resp := &pb.BatchGetDocumentsResponse{ReadTime: readTime}
		if doc, ok := docs[name]; ok {
			d, err := docProto(doc)
			if err != nil {
				return err
			}
			resp.Result = &pb.BatchGetDocumentsResponse_Found{Found: d}
		} else {
			resp.Result = &pb.BatchGetDocumentsResponse_Missing{Missing: name}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

// ListCollectionIds lists the collection ids under a parent, from the
// registry maintained on write.
func (s *Server) ListCollectionIds(ctx context.Context, req *pb.ListCollectionIdsRequest) (*pb.ListCollectionIdsResponse, error) {
	rows, err := s.store.Pool.Query(ctx,
		`SELECT collection_id FROM collections WHERE parent_path = $1 ORDER BY collection_id`, req.GetParent())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var resp pb.ListCollectionIdsResponse
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		resp.CollectionIds = append(resp.CollectionIds, id)
	}
	return &resp, rows.Err()
}

func docProto(doc *store.Doc) (*pb.Document, error) {
	fields := doc.Fields
	if fields == nil {
		fields = map[string]*pb.Value{}
	}
	return &pb.Document{
		Name:       doc.Name,
		Fields:     fields,
		CreateTime: timestamppb.New(doc.CreateTime),
		UpdateTime: timestamppb.New(doc.UpdateTime),
	}, nil
}

func statusProto(err error) *rpcstatus.Status {
	if err == nil {
		return &rpcstatus.Status{Code: int32(codes.OK)}
	}
	st, _ := status.FromError(err)
	return st.Proto()
}
