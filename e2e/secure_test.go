package e2e

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/omelas-tech/kor/internal/auth"
	"github.com/omelas-tech/kor/internal/pgtest"
	"github.com/omelas-tech/kor/internal/rpc"
	"github.com/omelas-tech/kor/internal/rules"
	"github.com/omelas-tech/kor/internal/store"
)

const secureProject = "demo-kor"

const secureRules = `rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    function signedIn() { return request.auth != null; }
    function chat(id) { return get(/databases/$(database)/documents/chats/$(id)).data; }
    match /chats/{chatId} {
      allow read: if signedIn() && request.auth.uid in resource.data.participants;
      allow update: if signedIn() && request.auth.uid in resource.data.participants &&
        request.resource.data.participants == resource.data.participants;
      match /messages/{m} {
        allow read: if signedIn() && request.auth.uid in chat(chatId).participants;
        allow create: if signedIn() && request.auth.uid in chat(chatId).participants &&
          request.resource.data.from == request.auth.uid;
      }
    }
    match /notes/{id} {
      allow read: if signedIn() && resource.data.owner == request.auth.uid;
      allow write: if signedIn() && request.resource.data.owner == request.auth.uid;
    }
  }
}`

type secureEnv struct {
	admin *firestore.Client
	key   *rsa.PrivateKey
	addr  string
}

func startSecure(t *testing.T) *secureEnv {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	rs, err := rules.Parse(secureRules)
	if err != nil {
		t.Fatal(err)
	}
	trusted := rpc.New(st)
	client := rpc.NewClient(trusted, rs, auth.New(secureProject, auth.StaticKeys{"k": &key.PublicKey}), secureProject, nil)

	serve := func(s *rpc.Server) string {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer()
		s.Register(g)
		go func() { _ = g.Serve(lis) }()
		t.Cleanup(g.Stop)
		return lis.Addr().String()
	}
	adminAddr := serve(trusted)
	env := &secureEnv{key: key, addr: serve(client)}
	env.admin = dialAs(t, adminAddr, "")
	return env
}

func (e *secureEnv) token(t *testing.T, uid string, mod func(map[string]any)) string {
	t.Helper()
	now := time.Now()
	c := map[string]any{"iss": "https://securetoken.google.com/" + secureProject, "aud": secureProject, "sub": uid,
		"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Hour).Unix()}
	if mod != nil {
		mod(c)
	}
	h, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": "k"})
	p, _ := json.Marshal(c)
	s := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	sum := sha256.Sum256([]byte(s))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, e.key, crypto.SHA256, sum[:])
	return s + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// dial connects the unmodified SDK, attaching token (if any) to every call
// the way Firebase client SDKs do.
func dialAs(t *testing.T, addr, token string) *firestore.Client {
	t.Helper()
	return dialAsFor(t, addr, token, secureProject)
}

func dialAsFor(t *testing.T, addr, token, project string) *firestore.Client {
	t.Helper()
	withToken := func(ctx context.Context) context.Context {
		if token == "" {
			return ctx
		}
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, m string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, o ...grpc.CallOption) error {
			return inv(withToken(ctx), m, req, reply, cc, o...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, d *grpc.StreamDesc, cc *grpc.ClientConn, m string, s grpc.Streamer, o ...grpc.CallOption) (grpc.ClientStream, error) {
			return s(withToken(ctx), d, cc, m, o...)
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	c, err := firestore.NewClient(context.Background(), project, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func wantCode(t *testing.T, what string, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("%s: got %v, want %v", what, err, code)
	}
}

func TestClientEndpointRules(t *testing.T) {
	ctx := context.Background()
	env := startSecure(t)
	if _, err := env.admin.Doc("chats/ab").Set(ctx, map[string]any{"participants": []any{"alice", "bob"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := env.admin.Doc("chats/cd").Set(ctx, map[string]any{"participants": []any{"carol", "dave"}}); err != nil {
		t.Fatal(err)
	}
	alice := dialAs(t, env.addr, env.token(t, "alice", nil))
	mallory := dialAs(t, env.addr, env.token(t, "mallory", nil))
	anon := dialAs(t, env.addr, "")

	// Document reads.
	if _, err := alice.Doc("chats/ab").Get(ctx); err != nil {
		t.Fatalf("participant get: %v", err)
	}
	_, err := mallory.Doc("chats/ab").Get(ctx)
	wantCode(t, "stranger get", err, codes.PermissionDenied)
	_, err = anon.Doc("chats/ab").Get(ctx)
	wantCode(t, "anonymous get", err, codes.PermissionDenied)
	_, err = alice.GetAll(ctx, []*firestore.DocumentRef{alice.Doc("chats/ab"), alice.Doc("chats/cd")})
	wantCode(t, "getAll with one forbidden doc", err, codes.PermissionDenied)

	// Queries: allowed only when the filter proves the rule.
	docs, err := alice.Collection("chats").Where("participants", "array-contains", "alice").Documents(ctx).GetAll()
	if err != nil || len(docs) != 1 || docs[0].Ref.ID != "ab" {
		t.Fatalf("my chats: %d docs, %v", len(docs), err)
	}
	_, err = alice.Collection("chats").Documents(ctx).GetAll()
	wantCode(t, "unfiltered chats", err, codes.PermissionDenied)
	_, err = alice.Collection("chats").Where("participants", "array-contains", "carol").Documents(ctx).GetAll()
	wantCode(t, "probing carol's chats", err, codes.PermissionDenied)
	mine := alice.Collection("chats").Where("participants", "array-contains", "alice")
	_, err = mine.NewAggregationQuery().WithCount("n").Get(ctx)
	if err != nil {
		t.Fatalf("count of my chats: %v", err)
	}
	_, err = alice.Collection("chats").NewAggregationQuery().WithCount("n").Get(ctx)
	wantCode(t, "count of all chats", err, codes.PermissionDenied)

	// Writes, with get() reading the parent chat.
	if _, err := alice.Doc("chats/ab/messages/m1").Create(ctx, map[string]any{"from": "alice", "text": "hi"}); err != nil {
		t.Fatalf("message create: %v", err)
	}
	_, err = alice.Doc("chats/ab/messages/m2").Create(ctx, map[string]any{"from": "bob", "text": "spoof"})
	wantCode(t, "spoofed sender", err, codes.PermissionDenied)
	_, err = mallory.Doc("chats/ab/messages/m3").Create(ctx, map[string]any{"from": "mallory"})
	wantCode(t, "stranger message", err, codes.PermissionDenied)

	// A batch is all or nothing: one denied write rejects the rest.
	b := alice.Batch()
	b.Set(alice.Doc("notes/n1"), map[string]any{"owner": "alice"})
	b.Set(alice.Doc("notes/n2"), map[string]any{"owner": "bob"})
	_, err = b.Commit(ctx)
	wantCode(t, "mixed batch", err, codes.PermissionDenied)
	if _, err := env.admin.Doc("notes/n1").Get(ctx); status.Code(err) != codes.NotFound {
		t.Fatalf("the allowed half of a denied batch was persisted: %v", err)
	}

	// Transactions run their writes through the same guard.
	err = alice.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		s, err := tx.Get(alice.Doc("chats/ab"))
		if err != nil {
			return err
		}
		return tx.Set(alice.Doc("chats/ab"), map[string]any{"participants": s.Data()["participants"], "topic": "x"})
	})
	if err != nil {
		t.Fatalf("allowed transaction: %v", err)
	}
	err = alice.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		return tx.Set(alice.Doc("chats/ab"), map[string]any{"participants": []any{"alice", "mallory"}})
	})
	wantCode(t, "transaction swapping a participant", err, codes.PermissionDenied)

	// Admin-only APIs are closed on the client endpoint.
	_, err = alice.Collections(ctx).Next()
	wantCode(t, "list collection ids", err, codes.PermissionDenied)

	// Bad tokens are UNAUTHENTICATED, not anonymous.
	forged := dialAs(t, env.addr, env.token(t, "alice", func(c map[string]any) { c["aud"] = "other-project" }))
	_, err = forged.Doc("chats/ab").Get(ctx)
	wantCode(t, "token for another project", err, codes.Unauthenticated)
	expired := dialAs(t, env.addr, env.token(t, "alice", func(c map[string]any) { c["exp"] = time.Now().Add(-time.Hour).Unix() }))
	_, err = expired.Doc("chats/ab").Get(ctx)
	wantCode(t, "expired token", err, codes.Unauthenticated)
}

func TestClientEndpointListen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	env := startSecure(t)
	if _, err := env.admin.Doc("chats/ab").Set(ctx, map[string]any{"participants": []any{"alice", "bob"}}); err != nil {
		t.Fatal(err)
	}
	alice := dialAs(t, env.addr, env.token(t, "alice", nil))
	bob := dialAs(t, env.addr, env.token(t, "bob", nil))
	mallory := dialAs(t, env.addr, env.token(t, "mallory", nil))

	// A participant's message listener sees the other side's writes live.
	it := alice.Collection("chats/ab/messages").Snapshots(ctx)
	defer it.Stop()
	if s, err := it.Next(); err != nil || s.Size != 0 {
		t.Fatalf("initial snapshot: %v", err)
	}
	if _, err := bob.Doc("chats/ab/messages/m1").Create(ctx, map[string]any{"from": "bob", "text": "hey"}); err != nil {
		t.Fatal(err)
	}
	s, err := it.Next()
	if err != nil || s.Size != 1 {
		t.Fatalf("live message: %v", err)
	}

	// A stranger's listener is refused.
	mit := mallory.Collection("chats/ab/messages").Snapshots(ctx)
	defer mit.Stop()
	_, err = mit.Next()
	wantCode(t, "stranger listen", err, codes.PermissionDenied)

	// A document listener loses access when the document changes hands.
	if _, err := env.admin.Doc("notes/n").Set(ctx, map[string]any{"owner": "alice"}); err != nil {
		t.Fatal(err)
	}
	dit := alice.Doc("notes/n").Snapshots(ctx)
	defer dit.Stop()
	if _, err := dit.Next(); err != nil {
		t.Fatalf("own note: %v", err)
	}
	if _, err := env.admin.Doc("notes/n").Set(ctx, map[string]any{"owner": "bob", "secret": "s"}); err != nil {
		t.Fatal(err)
	}
	snap, err := dit.Next()
	if err == nil {
		t.Fatalf("listener received the note after losing access: %v", snap.Data())
	}
	wantCode(t, "lost access", err, codes.PermissionDenied)

	_ = iterator.Done
	_ = pb.TargetChange_REMOVE
}
