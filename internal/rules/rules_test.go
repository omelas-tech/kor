package rules

import (
	"context"
	"testing"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/omelas-tech/kor/internal/query"
)

const corpus = `rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /{document=**} { allow read, write: if false; }

    function signedIn() { return request.auth != null; }
    function isUser(uid) { return signedIn() && request.auth.uid == uid; }
    function verified() {
      return signedIn() &&
        (request.auth.token.firebase.sign_in_provider != 'password' ||
         request.auth.token.email_verified == true);
    }
    function member(chatId) {
      let p = /databases/$(database)/documents/chats/$(chatId);
      return signedIn() && (
        (exists(p) && request.auth.uid in get(p).data.participants) ||
        (!exists(p) && (chatId.matches(request.auth.uid + '-.*') || chatId.matches('.*-' + request.auth.uid))));
    }
    function sized(field, max) {
      return field in request.resource.data && request.resource.data[field] is string &&
        request.resource.data[field].size() <= max;
    }

    match /chats/{chatId} {
      allow read: if signedIn() && (member(chatId) || (resource != null && request.auth.uid in resource.data.participants));
      allow create: if verified() &&
        request.resource.data.participants is list &&
        request.resource.data.participants.size() == 2 &&
        request.auth.uid in request.resource.data.participants &&
        (chatId == request.resource.data.participants[0] + '-' + request.resource.data.participants[1] ||
         chatId == request.resource.data.participants[1] + '-' + request.resource.data.participants[0]);
      allow update: if member(chatId) &&
        request.resource.data.participants.toSet() == resource.data.participants.toSet();
      allow delete: if signedIn() && resource != null && request.auth.uid in resource.data.participants;

      match /messages/{messageId} {
        allow read: if member(chatId);
        allow create: if member(chatId) && request.resource.data.senderId == request.auth.uid &&
          sized('text', 20) &&
          (!exists(/databases/$(database)/documents/chats/$(chatId)) ||
           !('last' in get(/databases/$(database)/documents/chats/$(chatId)).data) ||
           request.time > get(/databases/$(database)/documents/chats/$(chatId)).data.last + duration.value(1, 's'));
        allow update: if member(chatId) &&
          request.resource.data.diff(resource.data).affectedKeys().hasOnly(['readBy']) &&
          request.auth.uid in request.resource.data.readBy;
      }
    }
    match /presence/{uid} {
      allow read: if signedIn();
      allow write: if isUser(uid) && (request.method == 'delete' || request.resource.data.keys().hasOnly(['online', 'at']));
    }
    match /notes/{id} {
      allow read: if signedIn() && resource.data.owner == request.auth.uid;
      allow list: if request.query.limit <= 10 && resource.data.owner == request.auth.uid;
    }
    match /{path=**}/posts/{id} {
      allow read: if resource.data.public == true;
    }
    match /config/{id} { allow read; }
  }
}`

type memDocs map[Path]*Resource

func (m memDocs) Get(_ context.Context, p Path) (*Resource, error)      { return m[p], nil }
func (m memDocs) GetAfter(_ context.Context, p Path) (*Resource, error) { return m[p], nil }

const root = "projects/p/databases/(default)/documents"

func user(uid string, provider string, verified bool) map[string]any {
	return map[string]any{"uid": uid, "token": map[string]any{
		"email_verified": verified, "firebase": map[string]any{"sign_in_provider": provider}}}
}

func res(path string, data map[string]any) *Resource {
	p := Path("/databases/(default)/documents/" + path)
	return &Resource{Name: p, ID: p.last(), Data: data}
}

func mustParse(t *testing.T) *Ruleset {
	t.Helper()
	rs, err := Parse(corpus)
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

func TestDocumentDecisions(t *testing.T) {
	rs := mustParse(t)
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	chat := res("chats/alice-bob", map[string]any{"participants": []any{"alice", "bob"}, "last": now.Add(-10 * time.Second)})
	hot := res("chats/alice-carol", map[string]any{"participants": []any{"alice", "carol"}, "last": now.Add(-200 * time.Millisecond)})
	docs := memDocs{chat.Name: chat, hot.Name: hot}
	alice := user("alice", "google.com", false)
	mallory := user("mallory", "google.com", false)
	unverified := user("alice", "password", false)

	cases := []struct {
		name    string
		method  string
		path    string
		auth    map[string]any
		cur     *Resource
		next    *Resource
		allowed bool
	}{
		{"participant reads chat", "get", "chats/alice-bob", alice, chat, nil, true},
		{"stranger reads chat", "get", "chats/alice-bob", mallory, chat, nil, false},
		{"signed out reads chat", "get", "chats/alice-bob", nil, chat, nil, false},
		{"new chat by verified participant", "create", "chats/alice-dave", alice, nil,
			res("chats/alice-dave", map[string]any{"participants": []any{"alice", "dave"}}), true},
		{"new chat with forged id", "create", "chats/alice-zed", alice, nil,
			res("chats/alice-zed", map[string]any{"participants": []any{"alice", "dave"}}), false},
		{"unverified password account", "create", "chats/alice-dave", unverified, nil,
			res("chats/alice-dave", map[string]any{"participants": []any{"alice", "dave"}}), false},
		{"update keeps participants (reordered)", "update", "chats/alice-bob", alice, chat,
			res("chats/alice-bob", map[string]any{"participants": []any{"bob", "alice"}, "x": int64(1)}), true},
		{"update swaps a participant", "update", "chats/alice-bob", alice, chat,
			res("chats/alice-bob", map[string]any{"participants": []any{"alice", "mallory"}}), false},
		{"delete missing chat (resource null)", "delete", "chats/nope", alice, nil, nil, false},
		{"message within rate limit", "create", "chats/alice-bob/messages/m1", alice, nil,
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "alice", "text": "hi"}), true},
		{"message too fast", "create", "chats/alice-carol/messages/m1", alice, nil,
			res("chats/alice-carol/messages/m1", map[string]any{"senderId": "alice", "text": "hi"}), false},
		{"message spoofing sender", "create", "chats/alice-bob/messages/m1", alice, nil,
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "bob", "text": "hi"}), false},
		{"message too long", "create", "chats/alice-bob/messages/m1", alice, nil,
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "alice", "text": "0123456789012345678901"}), false},
		{"message missing text errors -> deny", "create", "chats/alice-bob/messages/m1", alice, nil,
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "alice"}), false},
		{"first message in a not-yet-created chat", "create", "chats/alice-erin/messages/m1", alice, nil,
			res("chats/alice-erin/messages/m1", map[string]any{"senderId": "alice", "text": "hi"}), true},
		{"mark read touches only readBy", "update", "chats/alice-bob/messages/m1", alice,
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "bob", "text": "hi", "readBy": []any{}}),
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "bob", "text": "hi", "readBy": []any{"alice"}}), true},
		{"edit text in an update", "update", "chats/alice-bob/messages/m1", alice,
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "bob", "text": "hi", "readBy": []any{}}),
			res("chats/alice-bob/messages/m1", map[string]any{"senderId": "bob", "text": "HACK", "readBy": []any{"alice"}}), false},
		{"presence owner write", "update", "presence/alice", alice, nil,
			res("presence/alice", map[string]any{"online": true}), true},
		{"presence extra field", "update", "presence/alice", alice, nil,
			res("presence/alice", map[string]any{"online": true, "admin": true}), false},
		{"presence delete by owner", "delete", "presence/alice", alice, nil, nil, true},
		{"presence other user", "update", "presence/bob", alice, nil,
			res("presence/bob", map[string]any{"online": true}), false},
		{"default deny", "get", "secrets/x", alice, res("secrets/x", map[string]any{}), nil, false},
		{"unconditional allow", "get", "config/app", nil, nil, nil, true},
		{"collection group rule on nested posts", "get", "users/u/posts/p", nil,
			res("users/u/posts/p", map[string]any{"public": true}), nil, true},
		{"collection group rule, private post", "get", "users/u/posts/p", nil,
			res("users/u/posts/p", map[string]any{"public": false}), nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := DocRequest(c.method, root+"/"+c.path, c.auth, now, c.cur, c.next, docs)
			d := rs.Evaluate(context.Background(), req, NewBudget(10))
			if d.Allowed != c.allowed {
				t.Fatalf("allowed=%v want %v (%s)", d.Allowed, c.allowed, d.Reason)
			}
		})
	}
}

func eqFilter(field string, v *pb.Value, op pb.StructuredQuery_FieldFilter_Operator) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{FieldFilter: &pb.StructuredQuery_FieldFilter{
		Field: &pb.StructuredQuery_FieldReference{FieldPath: field}, Op: op, Value: v}}}
}

func str(s string) *pb.Value { return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}} }

func TestQueryDecisions(t *testing.T) {
	rs := mustParse(t)
	alice := user("alice", "google.com", false)
	parse := func(parent, coll string, all bool, where *pb.StructuredQuery_Filter, limit int32) *query.Query {
		sq := &pb.StructuredQuery{From: []*pb.StructuredQuery_CollectionSelector{{CollectionId: coll, AllDescendants: all}}, Where: where}
		if limit > 0 {
			sq.Limit = wrapperspb.Int32(limit)
		}
		q, err := query.Parse(parent, sq)
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	and := func(fs ...*pb.StructuredQuery_Filter) *pb.StructuredQuery_Filter {
		return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_CompositeFilter{CompositeFilter: &pb.StructuredQuery_CompositeFilter{
			Op: pb.StructuredQuery_CompositeFilter_AND, Filters: fs}}}
	}
	eq := pb.StructuredQuery_FieldFilter_EQUAL
	ac := pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS
	cases := []struct {
		name    string
		q       *query.Query
		auth    map[string]any
		allowed bool
	}{
		{"chats where participants array-contains me", parse(root, "chats", false, eqFilter("participants", str("alice"), ac), 0), alice, true},
		{"chats probing another user", parse(root, "chats", false, eqFilter("participants", str("bob"), ac), 0), alice, false},
		{"all chats", parse(root, "chats", false, nil, 0), alice, false},
		{"my notes with a small limit", parse(root, "notes", false, eqFilter("owner", str("alice"), eq), 5), alice, true},
		{"my notes, no limit: allow read also covers list", parse(root, "notes", false, eqFilter("owner", str("alice"), eq), 0), alice, true},
		{"limit rule alone needs the limit", parse(root, "notes", false, eqFilter("tag", str("x"), eq), 5), alice, false},
		{"someone else's notes", parse(root, "notes", false, eqFilter("owner", str("bob"), eq), 5), alice, false},
		{"owner pinned inside an AND", parse(root, "notes", false, and(eqFilter("owner", str("alice"), eq), eqFilter("tag", str("x"), eq)), 5), alice, true},
		{"messages of a chat I am in (path rule)", parse(root+"/chats/alice-bob", "messages", false, nil, 0), alice, true},
		{"messages of someone else's chat", parse(root+"/chats/bob-carol", "messages", false, nil, 0), alice, false},
		{"collection group, public pinned", parse(root, "posts", true, eqFilter("public", &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: true}}, eq), 0), nil, true},
		{"collection group, unconstrained", parse(root, "posts", true, nil, 0), nil, false},
		{"presence list (rule ignores resource)", parse(root, "presence", false, nil, 0), alice, true},
		{"presence list signed out", parse(root, "presence", false, nil, 0), nil, false},
	}
	chat := res("chats/alice-bob", map[string]any{"participants": []any{"alice", "bob"}})
	other := res("chats/bob-carol", map[string]any{"participants": []any{"bob", "carol"}})
	docs := memDocs{chat.Name: chat, other.Name: other}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := QueryRequest(c.q, c.auth, time.Now())
			req.Docs = docs
			d := rs.Evaluate(context.Background(), req, NewBudget(10))
			if d.Allowed != c.allowed {
				t.Fatalf("allowed=%v want %v (%s)", d.Allowed, c.allowed, d.Reason)
			}
		})
	}
}

func TestLanguage(t *testing.T) {
	cases := map[string]bool{
		`1 + 2 == 3`:                true,
		`1 == 1.0`:                  true,
		`7 / 2 == 3 && 7 % 2 == 1`:  true,
		`'a' + 'b' == 'ab'`:         true,
		`[1, 2] + [3] == [1, 2, 3]`: true,
		`[1, 2, 3][1:] == [2, 3]`:   true,
		`{'a': 1}.keys() == ['a']`:  true,
		`{'a': 1, 'b': 2}.diff({'a': 1, 'c': 3}).affectedKeys() == ['b', 'c'].toSet()`:         true,
		`[1, 2].toSet().hasOnly([1, 2, 3])`:                                                    true,
		`[1, 2].hasAll([2])`:                                                                   true,
		`'Hello'.lower().matches('h.*o')`:                                                      true,
		`'abc'.matches('b')`:                                                                   false,
		`'a,b'.split(',') == ['a', 'b']`:                                                       true,
		`duration.value(90, 's') == duration.time(0, 1, 30, 0)`:                                true,
		`timestamp.date(2026, 11, 28).dayOfWeek() == 6`:                                        true,
		`1 is int && 1.5 is float && 1 is number && 'x' is string && [1] is list && {} is map`: true,
		`(true ? 'y' : 'n') == 'y'`:                                                            true,
		`{'a': 1}.get('b', 7) == 7`:                                                            true,
		`{'a': {'b': 2}}.get(['a', 'b'], 0) == 2`:                                              true,
		`math.abs(-3) == 3 && math.floor(1.7) == 1.0`:                                          true,
		`int('12') + 1 == 13 && string(5) == '5'`:                                              true,
		`true || {}.nope`:                                                                      true, // error absorbed
		`{}.nope || true`:                                                                      true, // commutative
		`false && {}.nope`:                                                                     false,
		`{}.nope == null`:                                                                      false, // error, denies
		`9223372036854775807 + 1 > 0`:                                                          false, // overflow error
		`'x' in {'x': 1} && 2 in [1, 2] && !(3 in [1, 2])`:                                     true,
		`1 < 'a'`: false, // error
	}
	for expr, want := range cases {
		rs, err := Parse(`rules_version = '2'; service cloud.firestore { match /databases/{d}/documents { match /t/{id} { allow get: if ` + expr + `; } } }`)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		d := rs.Evaluate(context.Background(), DocRequest("get", root+"/t/x", nil, time.Now(), nil, nil, nil), nil)
		if d.Allowed != want {
			t.Errorf("%s: allowed=%v want %v (%s)", expr, d.Allowed, want, d.Reason)
		}
	}
}

func TestReadBudget(t *testing.T) {
	rs, err := Parse(`rules_version = '2'; service cloud.firestore { match /databases/{d}/documents { match /t/{id} {
		allow get: if exists(/databases/$(d)/documents/a/1) && exists(/databases/$(d)/documents/a/2) &&
		              exists(/databases/$(d)/documents/a/1) && exists(/databases/$(d)/documents/a/3); } } }`)
	if err != nil {
		t.Fatal(err)
	}
	docs := memDocs{}
	for _, id := range []string{"1", "2", "3"} {
		r := res("a/"+id, map[string]any{})
		docs[r.Name] = r
	}
	if !rs.Evaluate(context.Background(), DocRequest("get", root+"/t/x", nil, time.Now(), nil, nil, docs), NewBudget(3)).Allowed {
		t.Fatal("3 distinct reads (one repeated) should fit a budget of 3")
	}
	if rs.Evaluate(context.Background(), DocRequest("get", root+"/t/x", nil, time.Now(), nil, nil, docs), NewBudget(2)).Allowed {
		t.Fatal("a 4th distinct read past the budget must deny")
	}
}

func TestParseErrors(t *testing.T) {
	for _, src := range []string{
		`service cloud.storage { }`,
		`rules_version = '3'; service cloud.firestore { }`,
		`service cloud.firestore { match /a/{b} { allow peek: if true; } }`,
		`service cloud.firestore { match /a/{b} { allow read: if (true; } }`,
	} {
		if _, err := Parse(src); err == nil {
			t.Errorf("parsed without error: %s", src)
		}
	}
}
