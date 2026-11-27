package rules

import (
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

func passthrough(vs ...any) (any, bool) {
	for _, v := range vs {
		if isErr(v) {
			return v, true
		}
	}
	for _, v := range vs {
		if isUnknown(v) || isSym(v) {
			return Unknown, true
		}
	}
	return nil, false
}

func unaryOp(op string, x any) any {
	if v, ok := passthrough(x); ok {
		return v
	}
	switch op {
	case "!":
		if b, ok := x.(bool); ok {
			return !b
		}
	case "-":
		switch v := x.(type) {
		case int64:
			if v == math.MinInt64 {
				return errf("integer overflow")
			}
			return -v
		case float64:
			return -v
		case Duration:
			return -v
		}
	}
	return errf("cannot apply %s to %s", op, typeName(x))
}

func binaryOp(op string, x, y any) any {
	if op == "in" {
		return inOp(x, y)
	}
	if op == "==" || op == "!=" {
		// A symbolic value stands for something that exists: never null.
		if (x == nil && isSym(y)) || (y == nil && isSym(x)) {
			return op == "!="
		}
	}
	if v, ok := passthrough(x, y); ok {
		return v
	}
	switch op {
	case "==":
		e, _ := equal(x, y)
		return e
	case "!=":
		e, _ := equal(x, y)
		return !e
	case "<", "<=", ">", ">=":
		c, err := compare(x, y)
		if err != nil {
			return err
		}
		switch op {
		case "<":
			return c < 0
		case "<=":
			return c <= 0
		case ">":
			return c > 0
		}
		return c >= 0
	case "+", "-", "*", "/", "%":
		return arith(op, x, y)
	}
	return errf("unknown operator %s", op)
}

func arith(op string, x, y any) any {
	switch a := x.(type) {
	case int64:
		switch b := y.(type) {
		case int64:
			return intArith(op, a, b)
		case float64:
			return floatArith(op, float64(a), b)
		}
	case float64:
		switch b := y.(type) {
		case int64:
			return floatArith(op, a, float64(b))
		case float64:
			return floatArith(op, a, b)
		}
	case string:
		if b, ok := y.(string); ok && op == "+" {
			return a + b
		}
	case []byte:
		if b, ok := y.([]byte); ok && op == "+" {
			return append(append([]byte{}, a...), b...)
		}
	case []any:
		if b, ok := y.([]any); ok && op == "+" {
			return append(append([]any{}, a...), b...)
		}
	case time.Time:
		switch b := y.(type) {
		case Duration:
			switch op {
			case "+":
				return a.Add(time.Duration(b))
			case "-":
				return a.Add(-time.Duration(b))
			}
		case time.Time:
			if op == "-" {
				return Duration(a.Sub(b))
			}
		}
	case Duration:
		switch b := y.(type) {
		case Duration:
			switch op {
			case "+":
				return a + b
			case "-":
				return a - b
			}
		case time.Time:
			if op == "+" {
				return b.Add(time.Duration(a))
			}
		}
	}
	return errf("cannot apply %s to %s and %s", op, typeName(x), typeName(y))
}

func intArith(op string, a, b int64) any {
	switch op {
	case "+":
		s := a + b
		if (a > 0 && b > 0 && s < 0) || (a < 0 && b < 0 && s >= 0) {
			return errf("integer overflow")
		}
		return s
	case "-":
		s := a - b
		if (a >= 0 && b < 0 && s < 0) || (a < 0 && b > 0 && s >= 0) {
			return errf("integer overflow")
		}
		return s
	case "*":
		if a == 0 || b == 0 {
			return int64(0)
		}
		p := a * b
		if p/b != a || (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) {
			return errf("integer overflow")
		}
		return p
	case "/":
		if b == 0 {
			return errf("division by zero")
		}
		if a == math.MinInt64 && b == -1 {
			return errf("integer overflow")
		}
		return a / b
	case "%":
		if b == 0 {
			return errf("modulus by zero")
		}
		if b == -1 {
			return int64(0)
		}
		return a % b
	}
	return errf("bad operator")
}

func abs64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}

func floatArith(op string, a, b float64) any {
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	case "/":
		return a / b
	}
	return errf("%% needs integers")
}

func inOp(x, y any) any {
	if isErr(x) {
		return x
	}
	if isErr(y) {
		return y
	}
	switch c := y.(type) {
	case unknown:
		return Unknown
	case []any:
		if isUnknown(x) {
			return Unknown
		}
		for _, el := range c {
			if e, _ := equal(x, el); e {
				return true
			}
		}
		return false
	case *Set:
		if isUnknown(x) {
			return Unknown
		}
		return c.has(x)
	case map[string]any:
		if isUnknown(x) {
			return Unknown
		}
		k, ok := x.(string)
		if !ok {
			return errf("map keys are strings, not %s", typeName(x))
		}
		_, has := c[k]
		return has
	case *symMap:
		k, ok := x.(string)
		if !ok {
			return Unknown
		}
		if _, has := c.field(k); has {
			return true
		}
		return Unknown
	case *symList:
		if isUnknown(x) {
			return Unknown
		}
		if e, _ := equal(x, c.contains); e {
			return true
		}
		return Unknown
	}
	return errf("cannot use in with %s", typeName(y))
}

func isType(v any, t string) any {
	got := typeName(v)
	switch t {
	case "number":
		return got == "int" || got == "float"
	case "constraint":
		return false
	case "bool", "bytes", "float", "int", "list", "latlng", "map", "path", "set", "string", "timestamp", "duration", "map_diff":
		return got == t
	}
	return errf("unknown type %s", t)
}

func member(x any, name string) any {
	switch v := x.(type) {
	case *Error, unknown:
		return v
	case nil:
		return errf("property %s on null", name)
	case map[string]any:
		if f, ok := v[name]; ok {
			return f
		}
		return errf("property %s is undefined", name)
	case *Resource:
		switch name {
		case "data":
			return v.Data
		case "id":
			return v.ID
		case "__name__":
			return v.Name
		}
		return errf("resource has no property %s", name)
	case *symMap:
		if f, ok := v.field(name); ok {
			return f
		}
		return Unknown
	case *symResource:
		switch name {
		case "data":
			return v.data
		}
		return Unknown
	}
	return errf("property %s on %s", name, typeName(x))
}

func index(x, i any) any {
	if v, ok := passthrough(x, i); ok {
		if isErr(v) || !isSym(x) {
			return v
		}
	}
	switch v := x.(type) {
	case []any:
		n, ok := i.(int64)
		if !ok {
			return errf("list index is %s, not int", typeName(i))
		}
		if n < 0 || n >= int64(len(v)) {
			return errf("list index %d out of range", n)
		}
		return v[n]
	case map[string]any, *symMap, *Resource, *symResource:
		k, ok := i.(string)
		if !ok {
			if isUnknown(i) {
				return Unknown
			}
			return errf("map index is %s, not string", typeName(i))
		}
		return member(v, k)
	case Path:
		n, ok := i.(int64)
		segs := v.segments()
		if !ok || n < 0 || n >= int64(len(segs)) {
			return errf("bad path index")
		}
		return segs[n]
	case *symList:
		return Unknown
	}
	return errf("cannot index %s", typeName(x))
}

func isSym(x any) bool {
	switch x.(type) {
	case *symMap, *symList, *symResource:
		return true
	}
	return false
}

func slice(x, lo, hi any) any {
	if v, ok := passthrough(x, lo, hi); ok {
		return v
	}
	l, ok := x.([]any)
	if !ok {
		if s, isStr := x.(string); isStr {
			// rules allow string ranges too (by character)
			r := []rune(s)
			a, b, err := bounds(lo, hi, len(r))
			if err != nil {
				return err
			}
			return string(r[a:b])
		}
		return errf("cannot slice %s", typeName(x))
	}
	a, b, err := bounds(lo, hi, len(l))
	if err != nil {
		return err
	}
	return append([]any{}, l[a:b]...)
}

func bounds(lo, hi any, n int) (int, int, *Error) {
	a, b := 0, n
	if lo != nil {
		v, ok := lo.(int64)
		if !ok {
			return 0, 0, errf("range bound is not int")
		}
		a = int(v)
	}
	if hi != nil {
		v, ok := hi.(int64)
		if !ok {
			return 0, 0, errf("range bound is not int")
		}
		b = int(v)
	}
	if a < 0 || b > n || a > b {
		return 0, 0, errf("range [%d:%d] out of bounds", a, b)
	}
	return a, b, nil
}

var reCache sync.Map

func compileRE(re string) (*regexp.Regexp, *Error) {
	if c, ok := reCache.Load(re); ok {
		return c.(*regexp.Regexp), nil
	}
	c, err := regexp.Compile("^(?:" + re + ")$")
	if err != nil {
		return nil, errf("bad regex %q: %v", re, err)
	}
	reCache.Store(re, c)
	return c, nil
}

func listOf(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	case *Set:
		return t.items, true
	}
	return nil, false
}

func method(recv any, name string, args []any) any {
	if isUnknown(recv) || isSym(recv) {
		if m, ok := recv.(*symMap); ok && name == "get" && len(args) == 2 {
			if k, isStr := args[0].(string); isStr {
				if f, has := m.field(k); has {
					return f
				}
			}
		}
		if l, ok := recv.(*symList); ok && name == "hasAny" && len(args) == 1 {
			if items, isList := listOf(args[0]); isList {
				for _, it := range items {
					if e, _ := equal(it, l.contains); e {
						return true
					}
				}
			}
		}
		return Unknown
	}
	for _, a := range args {
		if isUnknown(a) {
			return Unknown
		}
	}
	arity := func(n int) *Error {
		if len(args) != n {
			return errf("%s.%s takes %d arguments", typeName(recv), name, n)
		}
		return nil
	}
	switch v := recv.(type) {
	case string:
		switch name {
		case "size":
			return int64(utf8.RuneCountInString(v))
		case "matches":
			if e := arity(1); e != nil {
				return e
			}
			re, ok := args[0].(string)
			if !ok {
				return errf("matches takes a string")
			}
			c, err := compileRE(re)
			if err != nil {
				return err
			}
			return c.MatchString(v)
		case "lower":
			return strings.ToLower(v)
		case "upper":
			return strings.ToUpper(v)
		case "trim":
			return strings.TrimSpace(v)
		case "split":
			if e := arity(1); e != nil {
				return e
			}
			re, ok := args[0].(string)
			if !ok {
				return errf("split takes a string")
			}
			c, err := regexp.Compile(re)
			if err != nil {
				return errf("bad regex %q", re)
			}
			parts := c.Split(v, -1)
			out := make([]any, len(parts))
			for i, p := range parts {
				out[i] = p
			}
			return out
		case "replace":
			if e := arity(2); e != nil {
				return e
			}
			re, ok1 := args[0].(string)
			sub, ok2 := args[1].(string)
			if !ok1 || !ok2 {
				return errf("replace takes strings")
			}
			c, err := regexp.Compile(re)
			if err != nil {
				return errf("bad regex %q", re)
			}
			return c.ReplaceAllString(v, sub)
		case "toUtf8":
			return []byte(v)
		}
	case []byte:
		if name == "size" {
			return int64(len(v))
		}
	case []any:
		switch name {
		case "size":
			return int64(len(v))
		case "toSet":
			return newSet(v)
		case "join":
			if e := arity(1); e != nil {
				return e
			}
			sep, ok := args[0].(string)
			if !ok {
				return errf("join takes a string")
			}
			parts := make([]string, len(v))
			for i, el := range v {
				s, ok := el.(string)
				if !ok {
					return errf("join needs a list of strings")
				}
				parts[i] = s
			}
			return strings.Join(parts, sep)
		case "concat":
			if e := arity(1); e != nil {
				return e
			}
			o, ok := args[0].([]any)
			if !ok {
				return errf("concat takes a list")
			}
			return append(append([]any{}, v...), o...)
		case "removeAll":
			if e := arity(1); e != nil {
				return e
			}
			o, ok := listOf(args[0])
			if !ok {
				return errf("removeAll takes a list")
			}
			rm := newSet(o)
			var out []any
			for _, el := range v {
				if !rm.has(el) {
					out = append(out, el)
				}
			}
			if out == nil {
				out = []any{}
			}
			return out
		case "hasAll", "hasAny", "hasOnly":
			return setPredicate(name, newSet(v), args)
		}
	case *Set:
		switch name {
		case "size":
			return int64(len(v.items))
		case "hasAll", "hasAny", "hasOnly":
			return setPredicate(name, v, args)
		case "union", "intersection", "difference":
			if e := arity(1); e != nil {
				return e
			}
			o, ok := listOf(args[0])
			if !ok {
				return errf("%s takes a set", name)
			}
			other := newSet(o)
			var out []any
			switch name {
			case "union":
				out = append(append([]any{}, v.items...), other.items...)
			case "intersection":
				for _, it := range v.items {
					if other.has(it) {
						out = append(out, it)
					}
				}
			case "difference":
				for _, it := range v.items {
					if !other.has(it) {
						out = append(out, it)
					}
				}
			}
			return newSet(out)
		}
	case map[string]any:
		switch name {
		case "size":
			return int64(len(v))
		case "keys":
			ks := make([]string, 0, len(v))
			for k := range v {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			out := make([]any, len(ks))
			for i, k := range ks {
				out[i] = k
			}
			return out
		case "values":
			ks := make([]string, 0, len(v))
			for k := range v {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			out := make([]any, len(ks))
			for i, k := range ks {
				out[i] = v[k]
			}
			return out
		case "get":
			if e := arity(2); e != nil {
				return e
			}
			switch k := args[0].(type) {
			case string:
				if f, ok := v[k]; ok {
					return f
				}
				return args[1]
			case []any:
				var cur any = v
				for _, part := range k {
					m, ok := cur.(map[string]any)
					ks, isStr := part.(string)
					if !ok || !isStr {
						return args[1]
					}
					if cur, ok = m[ks]; !ok {
						return args[1]
					}
				}
				return cur
			}
			return errf("map.get takes a key or a list of keys")
		case "diff":
			if e := arity(1); e != nil {
				return e
			}
			o, ok := args[0].(map[string]any)
			if !ok {
				return errf("diff takes a map, got %s", typeName(args[0]))
			}
			return &MapDiff{left: v, right: o}
		}
	case *MapDiff:
		switch name {
		case "addedKeys":
			return v.keys(func(_ string, inL, inR, _ bool) bool { return inL && !inR })
		case "removedKeys":
			return v.keys(func(_ string, inL, inR, _ bool) bool { return !inL && inR })
		case "changedKeys":
			return v.keys(func(_ string, inL, inR, same bool) bool { return inL && inR && !same })
		case "unchangedKeys":
			return v.keys(func(_ string, inL, inR, same bool) bool { return inL && inR && same })
		case "affectedKeys":
			return v.keys(func(_ string, inL, inR, same bool) bool { return !(inL && inR && same) })
		}
	case time.Time:
		u := v.UTC()
		switch name {
		case "year":
			return int64(u.Year())
		case "month":
			return int64(u.Month())
		case "day":
			return int64(u.Day())
		case "hours":
			return int64(u.Hour())
		case "minutes":
			return int64(u.Minute())
		case "seconds":
			return int64(u.Second())
		case "nanos":
			return int64(u.Nanosecond())
		case "dayOfWeek":
			wd := int64(u.Weekday())
			if wd == 0 {
				wd = 7
			}
			return wd
		case "dayOfYear":
			return int64(u.YearDay())
		case "toMillis":
			return u.UnixMilli()
		case "date":
			return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
		case "time":
			return Duration(u.Sub(time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)))
		}
	case Duration:
		switch name {
		case "seconds":
			return int64(time.Duration(v) / time.Second)
		case "nanos":
			return int64(time.Duration(v) % time.Second)
		}
	case LatLng:
		switch name {
		case "latitude":
			return v.Lat
		case "longitude":
			return v.Lng
		case "distance":
			if e := arity(1); e != nil {
				return e
			}
			o, ok := args[0].(LatLng)
			if !ok {
				return errf("distance takes a latlng")
			}
			return haversine(v, o)
		}
	case Path:
		if name == "bind" {
			if e := arity(1); e != nil {
				return e
			}
			m, ok := args[0].(map[string]any)
			if !ok {
				return errf("bind takes a map")
			}
			s := string(v)
			for k, val := range m {
				if str, ok := val.(string); ok {
					s = strings.ReplaceAll(s, "{"+k+"}", str)
				}
			}
			return Path(s)
		}
	}
	return errf("%s has no method %s/%d", typeName(recv), name, len(args))
}

func setPredicate(name string, s *Set, args []any) any {
	if len(args) != 1 {
		return errf("%s takes one list", name)
	}
	o, ok := listOf(args[0])
	if !ok {
		return errf("%s takes a list or set, got %s", name, typeName(args[0]))
	}
	switch name {
	case "hasAll":
		for _, it := range o {
			if !s.has(it) {
				return false
			}
		}
		return true
	case "hasAny":
		for _, it := range o {
			if s.has(it) {
				return true
			}
		}
		return false
	}
	other := newSet(o)
	for _, it := range s.items {
		if !other.has(it) {
			return false
		}
	}
	return true
}

func namespaceCall(ns, name string, args []any) any {
	num := func(i int) (float64, bool) {
		switch v := args[i].(type) {
		case int64:
			return float64(v), true
		case float64:
			return v, true
		}
		return 0, false
	}
	switch ns + "." + name {
	case "duration.value":
		if len(args) == 2 {
			n, ok := args[0].(int64)
			unit, ok2 := args[1].(string)
			if ok && ok2 {
				mult := map[string]time.Duration{"w": 7 * 24 * time.Hour, "d": 24 * time.Hour, "h": time.Hour, "m": time.Minute, "s": time.Second, "ms": time.Millisecond, "ns": time.Nanosecond}[unit]
				if mult == 0 {
					return errf("unknown duration unit %q", unit)
				}
				return Duration(time.Duration(n) * mult)
			}
		}
	case "duration.time":
		if len(args) == 4 {
			var parts [4]int64
			for i := range args {
				v, ok := args[i].(int64)
				if !ok {
					return errf("duration.time takes ints")
				}
				parts[i] = v
			}
			return Duration(time.Duration(parts[0])*time.Hour + time.Duration(parts[1])*time.Minute + time.Duration(parts[2])*time.Second + time.Duration(parts[3]))
		}
	case "duration.abs":
		if len(args) == 1 {
			if d, ok := args[0].(Duration); ok {
				if d < 0 {
					return -d
				}
				return d
			}
		}
	case "timestamp.date":
		if len(args) == 3 {
			y, ok1 := args[0].(int64)
			m, ok2 := args[1].(int64)
			d, ok3 := args[2].(int64)
			if ok1 && ok2 && ok3 {
				return time.Date(int(y), time.Month(m), int(d), 0, 0, 0, 0, time.UTC)
			}
		}
	case "timestamp.value":
		if len(args) == 1 {
			if ms, ok := args[0].(int64); ok {
				return time.UnixMilli(ms).UTC()
			}
		}
	case "latlng.value":
		if len(args) == 2 {
			a, ok1 := num(0)
			b, ok2 := num(1)
			if ok1 && ok2 {
				return LatLng{a, b}
			}
		}
	case "math.abs":
		if len(args) == 1 {
			switch v := args[0].(type) {
			case int64:
				if v == math.MinInt64 {
					return errf("integer overflow")
				}
				return abs64(v)
			case float64:
				return math.Abs(v)
			}
		}
	case "math.ceil", "math.floor", "math.round", "math.sqrt", "math.isInfinite", "math.isNaN":
		if len(args) == 1 {
			f, ok := num(0)
			if !ok {
				break
			}
			switch name {
			case "ceil":
				return math.Ceil(f)
			case "floor":
				return math.Floor(f)
			case "round":
				return math.Round(f)
			case "sqrt":
				return math.Sqrt(f)
			case "isInfinite":
				return math.IsInf(f, 0)
			case "isNaN":
				return math.IsNaN(f)
			}
		}
	case "math.pow":
		if len(args) == 2 {
			a, ok1 := num(0)
			b, ok2 := num(1)
			if ok1 && ok2 {
				return math.Pow(a, b)
			}
		}
	}
	return errf("unknown function %s.%s/%d", ns, name, len(args))
}

func toString(v any) any {
	switch t := v.(type) {
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return "null"
	case Path:
		return string(t)
	}
	return errf("string() of %s", typeName(v))
}

func haversine(a, b LatLng) float64 {
	const r = 6371008.8
	la1, la2 := a.Lat*math.Pi/180, b.Lat*math.Pi/180
	dla, dlo := la2-la1, (b.Lng-a.Lng)*math.Pi/180
	h := math.Sin(dla/2)*math.Sin(dla/2) + math.Cos(la1)*math.Cos(la2)*math.Sin(dlo/2)*math.Sin(dlo/2)
	return 2 * r * math.Asin(math.Sqrt(h))
}
