package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omelas-tech/kor/internal/query"
	"github.com/omelas-tech/kor/internal/store"
)

// Listen implements the realtime watch stream the client SDKs use for
// onSnapshot / Snapshots(), and which the native mobile SDKs also use for a
// plain server get().
//
// Per target the protocol is: ADD, the matching documents, CURRENT, then a
// global NO_CHANGE (empty target ids, read_time) that tells the client its
// view is consistent and it may raise a snapshot. Every later change is a
// DocumentChange / DocumentDelete / DocumentRemove followed by another global
// NO_CHANGE.
//
// Resume tokens are accepted but not used to replay: a client that resumes
// gets RESET and the full current result. That is always correct (the client
// drops what it held and takes the new set), where an incremental resume
// would have to know which documents left the result while it was away.
func (s *Server) Listen(stream pb.Firestore_ListenServer) error {
	ctx := stream.Context()
	sub, err := s.hub.subscribe(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "kor: listen: %v", err)
	}
	defer s.hub.unsubscribe(sub)

	reqs := make(chan *pb.ListenRequest)
	recvErr := make(chan error, 1)
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case reqs <- r:
			case <-ctx.Done():
				return
			}
		}
	}()

	l := &listener{srv: s, stream: stream, targets: map[int32]*watchTarget{}}
	defer func() { listenTargets.Sub(float64(len(l.targets))) }()
	for {
		select {
		case r := <-reqs:
			if err := l.handleRequest(ctx, r); err != nil {
				return err
			}
		case batch := <-sub.ch:
			if err := l.apply(ctx, batch); err != nil {
				return err
			}
		case <-sub.lost:
			return status.Error(codes.Unavailable, "kor: listener fell behind the change feed; reconnect to resync")
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type listener struct {
	srv      *Server
	stream   pb.Firestore_ListenServer
	targets  map[int32]*watchTarget
	lastRead time.Time
	tokenSeq uint64
}

// watchTarget is one target's server-side view: what the client currently
// holds for it, so a change is sent only when it alters that view.
type watchTarget struct {
	id    int32
	q     *query.Query    // nil for a documents target
	docs  map[string]bool // documents target: the watched names
	known map[string]time.Time
	once  bool
	// rerun: a limit or offset makes membership depend on other documents
	// (a delete pulls the next one into the window), so the query re-runs and
	// diffs instead of judging the changed document alone.
	rerun bool
}

func (l *listener) handleRequest(ctx context.Context, r *pb.ListenRequest) error {
	switch t := r.GetTargetChange().(type) {
	case *pb.ListenRequest_AddTarget:
		return l.addTarget(ctx, t.AddTarget)
	case *pb.ListenRequest_RemoveTarget:
		l.removeTarget(t.RemoveTarget)
		return l.send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_TargetChange{TargetChange: &pb.TargetChange{
			TargetChangeType: pb.TargetChange_REMOVE, TargetIds: []int32{t.RemoveTarget},
		}}})
	default:
		return status.Error(codes.InvalidArgument, "kor: listen request without add_target or remove_target")
	}
}

func (l *listener) removeTarget(id int32) {
	if _, ok := l.targets[id]; ok {
		delete(l.targets, id)
		listenTargets.Dec()
	}
}

func (l *listener) addTarget(ctx context.Context, pt *pb.Target) error {
	id := pt.GetTargetId()
	wt := &watchTarget{id: id, known: map[string]time.Time{}, once: pt.GetOnce()}
	var parseErr error
	switch tt := pt.GetTargetType().(type) {
	case *pb.Target_Query:
		wt.q, parseErr = query.Parse(tt.Query.GetParent(), tt.Query.GetStructuredQuery())
		if parseErr == nil {
			wt.rerun = wt.q.Limit >= 0 || wt.q.Offset > 0
		}
	case *pb.Target_Documents:
		wt.docs = map[string]bool{}
		for _, n := range tt.Documents.GetDocuments() {
			wt.docs[n] = true
		}
	default:
		parseErr = status.Error(codes.InvalidArgument, "kor: target has neither a query nor documents")
	}
	if parseErr != nil {
		// A bad target is reported on the target, not the stream: the client
		// surfaces it to that one listener and the others keep working.
		st, _ := status.FromError(parseErr)
		return l.send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_TargetChange{TargetChange: &pb.TargetChange{
			TargetChangeType: pb.TargetChange_REMOVE, TargetIds: []int32{id}, Cause: st.Proto(),
		}}})
	}

	l.removeTarget(id) // re-adding an id replaces it
	if err := l.targetChange(pb.TargetChange_ADD, id, nil); err != nil {
		return err
	}
	if len(pt.GetResumeToken()) > 0 || pt.GetReadTime() != nil {
		if err := l.targetChange(pb.TargetChange_RESET, id, nil); err != nil {
			return err
		}
	}
	docs, err := l.current(ctx, wt)
	if err != nil {
		return err
	}
	for _, d := range docs {
		if err := l.sendDoc(wt, d); err != nil {
			return err
		}
	}
	token := l.nextToken()
	if err := l.targetChange(pb.TargetChange_CURRENT, id, token); err != nil {
		return err
	}
	if err := l.consistent(token); err != nil {
		return err
	}
	if wt.once {
		return l.targetChange(pb.TargetChange_REMOVE, id, nil)
	}
	l.targets[id] = wt
	listenTargets.Inc()
	return nil
}

// current returns the target's full result as of now.
func (l *listener) current(ctx context.Context, wt *watchTarget) ([]*store.Doc, error) {
	if wt.q == nil {
		names := make([]string, 0, len(wt.docs))
		for n := range wt.docs {
			names = append(names, n)
		}
		found, err := l.srv.store.GetDocuments(ctx, names)
		if err != nil {
			return nil, err
		}
		out := make([]*store.Doc, 0, len(found))
		for _, d := range found {
			out = append(out, d)
		}
		return out, nil
	}
	var out []*store.Doc
	err := l.srv.store.RunQuery(ctx, wt.q, func(d *store.Doc) error {
		out = append(out, d)
		return nil
	})
	return out, err
}

// apply folds one batch of committed changes into every target.
func (l *listener) apply(ctx context.Context, batch []store.Change) error {
	if len(l.targets) == 0 {
		return nil
	}
	relevant := map[*watchTarget][]string{}
	var names []string
	seen := map[string]bool{}
	for _, c := range batch {
		for _, wt := range l.targets {
			if wt.covers(c.DocName, c.CollectionID) {
				relevant[wt] = append(relevant[wt], c.DocName)
				if !seen[c.DocName] {
					seen[c.DocName] = true
					names = append(names, c.DocName)
				}
			}
		}
	}
	if len(relevant) == 0 {
		return nil
	}
	// Current state, not the event's: the feed carries no data by default,
	// and re-reading converges however many events were coalesced.
	docs, err := l.srv.store.GetDocuments(ctx, names)
	if err != nil {
		return err
	}
	changed := false
	for wt, ns := range relevant {
		var c bool
		if wt.rerun {
			c, err = l.rediff(ctx, wt, docs)
		} else {
			c, err = l.judge(wt, ns, docs)
		}
		if err != nil {
			return err
		}
		changed = changed || c
	}
	if !changed {
		return nil
	}
	return l.consistent(l.nextToken())
}

// judge decides membership from each changed document alone.
func (l *listener) judge(wt *watchTarget, names []string, docs map[string]*store.Doc) (bool, error) {
	changed := false
	for _, n := range names {
		d := docs[n]
		in := d != nil && (wt.q == nil || (wt.q.Matches(d.Name, d.Fields) && wt.q.InCursorRange(wt.q.OrderKey(d.Name, d.Fields))))
		c, err := l.reconcile(wt, n, d, in)
		if err != nil {
			return false, err
		}
		changed = changed || c
	}
	return changed, nil
}

// rediff re-runs a limit/offset query and diffs it against what the client
// holds. docs is the batch's re-read state, used to tell a delete from a
// document that merely left the window.
func (l *listener) rediff(ctx context.Context, wt *watchTarget, docs map[string]*store.Doc) (bool, error) {
	result, err := l.current(ctx, wt)
	if err != nil {
		return false, err
	}
	changed := false
	now := make(map[string]bool, len(result))
	for _, d := range result {
		now[d.Name] = true
		c, err := l.reconcile(wt, d.Name, d, true)
		if err != nil {
			return false, err
		}
		changed = changed || c
	}
	for n := range wt.known {
		if now[n] {
			continue
		}
		d, inBatch := docs[n]
		if !inBatch {
			d = &store.Doc{Name: n} // still exists; it was pushed out of the window
		}
		c, err := l.reconcile(wt, n, d, false)
		if err != nil {
			return false, err
		}
		changed = changed || c
	}
	return changed, nil
}

// reconcile sends what moves the client from its view to the truth for one
// document, and reports whether anything was sent.
func (l *listener) reconcile(wt *watchTarget, name string, d *store.Doc, in bool) (bool, error) {
	old, had := wt.known[name]
	switch {
	case in && (!had || !old.Equal(d.UpdateTime)):
		return true, l.sendDoc(wt, d)
	case !in && had:
		delete(wt.known, name)
		if d == nil {
			return true, l.send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_DocumentDelete{DocumentDelete: &pb.DocumentDelete{
				Document: name, RemovedTargetIds: []int32{wt.id}, ReadTime: l.readTime(),
			}}})
		}
		return true, l.send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_DocumentRemove{DocumentRemove: &pb.DocumentRemove{
			Document: name, RemovedTargetIds: []int32{wt.id}, ReadTime: l.readTime(),
		}}})
	}
	return false, nil
}

func (l *listener) sendDoc(wt *watchTarget, d *store.Doc) error {
	pd, err := docProto(d)
	if err != nil {
		return err
	}
	if wt.q != nil {
		pd.Fields = wt.q.ApplyProjection(pd.Fields)
	}
	wt.known[d.Name] = d.UpdateTime
	return l.send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_DocumentChange{DocumentChange: &pb.DocumentChange{
		Document: pd, TargetIds: []int32{wt.id},
	}}})
}

// covers reports whether a change to name can affect the target.
func (wt *watchTarget) covers(name, collectionID string) bool {
	if wt.q == nil {
		return wt.docs[name]
	}
	if _, had := wt.known[name]; had {
		return true
	}
	if collectionID != wt.q.CollectionID {
		return false
	}
	if wt.q.AllDescendants {
		return strings.HasPrefix(name, wt.q.Parent+"/")
	}
	return parentOf(name) == wt.q.Parent
}

// parentOf returns the resource that holds a document's collection:
// ".../documents/a/x/b/y" -> ".../documents/a/x"; a root doc -> ".../documents".
func parentOf(name string) string {
	i := strings.LastIndexByte(name, '/')
	if i < 0 {
		return ""
	}
	j := strings.LastIndexByte(name[:i], '/')
	if j < 0 {
		return ""
	}
	return name[:j]
}

func (l *listener) targetChange(kind pb.TargetChange_TargetChangeType, id int32, token []byte) error {
	return l.send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_TargetChange{TargetChange: &pb.TargetChange{
		TargetChangeType: kind, TargetIds: []int32{id}, ResumeToken: token,
	}}})
}

// consistent is the global NO_CHANGE marker: every target is up to date as
// of read_time, so the client may raise a snapshot.
func (l *listener) consistent(token []byte) error {
	return l.send(&pb.ListenResponse{ResponseType: &pb.ListenResponse_TargetChange{TargetChange: &pb.TargetChange{
		TargetChangeType: pb.TargetChange_NO_CHANGE, ReadTime: l.readTime(), ResumeToken: token,
	}}})
}

// readTime is wall-clock, clamped so it never goes backwards on a stream:
// the mobile SDKs reject a snapshot older than one they already raised.
func (l *listener) readTime() *timestamppb.Timestamp {
	now := time.Now().UTC().Truncate(time.Microsecond)
	if !now.After(l.lastRead) {
		now = l.lastRead.Add(time.Microsecond)
	}
	l.lastRead = now
	return timestamppb.New(now)
}

func (l *listener) nextToken() []byte {
	l.tokenSeq++
	return []byte(fmt.Sprintf("kor1:%d:%d", l.lastRead.UnixMicro(), l.tokenSeq))
}

func (l *listener) send(r *pb.ListenResponse) error { return l.stream.Send(r) }
