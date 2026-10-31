package kinds

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

//go:embed lr.json
var lrTable []byte

// Schema is one typed kind of the long-running drive, lr.<name>: its required and optional
// fields with their shapes, the fields that identify a record, and the audience it may be
// addressed to. A body that does not fit is refused at post with the command that would.
type Schema struct {
	Fields   map[string]json.RawMessage `json:"fields"`
	Optional map[string]json.RawMessage `json:"optional"`
	Identity []string                   `json:"identity"`
	Audience json.RawMessage            `json:"audience"`
	order    []string
}

const typedNamespace = "lr."

var (
	typed      map[string]Schema
	typedOrder []string
	shaText    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	timeText   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`)
)

func init() {
	typed = map[string]Schema{}
	for _, entry := range orderedObject(lrTable) {
		var s Schema
		if err := json.Unmarshal(entry.value, &s); err != nil {
			panic(err)
		}
		var outer struct {
			Fields json.RawMessage `json:"fields"`
		}
		if err := json.Unmarshal(entry.value, &outer); err != nil {
			panic(err)
		}
		for _, field := range orderedObject(outer.Fields) {
			s.order = append(s.order, field.key)
		}
		typed[entry.key] = s
		typedOrder = append(typedOrder, entry.key)
	}
}

type member struct {
	key   string
	value json.RawMessage
}

func orderedObject(raw json.RawMessage) []member {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if _, err := dec.Token(); err != nil {
		panic(err)
	}
	var out []member
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			panic(err)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			panic(err)
		}
		out = append(out, member{tok.(string), value})
	}
	return out
}

// Typed returns the schema of a typed kind, lr.<name>, and whether k is one.
func Typed(k Kind) (Schema, bool) {
	name, ok := strings.CutPrefix(string(k), typedNamespace)
	if !ok {
		return Schema{}, false
	}
	s, ok := typed[name]
	return s, ok
}

// TypedNames lists the typed kinds in table order.
func TypedNames() []string {
	return slices.Clone(typedOrder)
}

// CheckBody refuses a typed kind's body that is missing, has a field missing, extra or of the
// wrong shape, names several PRs, or is addressed to an audience the kind never reaches; the
// error carries the corrected command. An untyped kind passes.
func CheckBody(k Kind, body json.RawMessage, to []string) error {
	s, ok := Typed(k)
	if !ok {
		return nil
	}
	name := strings.TrimPrefix(string(k), typedNamespace)
	if len(body) == 0 {
		return fmt.Errorf("%s needs --body-file with %s", name, strings.Join(s.order, ", "))
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		return fmt.Errorf("%s body must be a JSON object: %w", name, err)
	}
	given := make([]string, 0, len(fields))
	for f := range fields {
		given = append(given, f)
	}
	if !s.accepts(given) {
		for _, other := range typedOrder {
			if other != name && typed[other].accepts(given) {
				return fmt.Errorf("these fields are a %s record, not %s; run: %s", other, name, command(other, fields, to))
			}
		}
		return fmt.Errorf("%s", s.mismatch(name, given))
	}
	for _, f := range append(slices.Clone(s.order), optionalNames(s)...) {
		value, ok := fields[f]
		if !ok {
			continue
		}
		if prs, many := value.([]any); f == "pr" && many {
			cmds := make([]string, 0, len(prs))
			for _, pr := range prs {
				one := map[string]any{}
				for k2, v2 := range fields {
					one[k2] = v2
				}
				one["pr"] = pr
				cmds = append(cmds, command(name, one, to))
			}
			return fmt.Errorf("a %s names one PR; post one record per PR: %s", name, strings.Join(cmds, " && "))
		}
		shape := s.Fields[f]
		if shape == nil {
			shape = s.Optional[f]
		}
		if !fits(shape, value) {
			return fmt.Errorf("%s.%s must be %s, got %s", name, f, describe(shape), jsonText(value))
		}
	}
	if string(s.Audience) == `"any"` {
		return nil
	}
	var audience []string
	if err := json.Unmarshal(s.Audience, &audience); err != nil {
		panic(err)
	}
	for _, lane := range to {
		if !slices.Contains(audience, lane) {
			msg := fmt.Sprintf("a %s record never reaches %s; it goes to %s", name, lane, strings.Join(audience, ", "))
			if lane == "root" {
				msg += "; to tell root, post an escalation, an ask or a decision"
			}
			return fmt.Errorf("%s", msg)
		}
	}
	return nil
}

func (s Schema) accepts(given []string) bool {
	for _, f := range s.order {
		if !slices.Contains(given, f) {
			return false
		}
	}
	for _, f := range given {
		if _, req := s.Fields[f]; req {
			continue
		}
		if _, opt := s.Optional[f]; !opt {
			return false
		}
	}
	return true
}

func (s Schema) mismatch(name string, given []string) string {
	var missing, extra []string
	for _, f := range s.order {
		if !slices.Contains(given, f) {
			missing = append(missing, f)
		}
	}
	for _, f := range given {
		if _, req := s.Fields[f]; req {
			continue
		}
		if _, opt := s.Optional[f]; !opt {
			extra = append(extra, f)
		}
	}
	sort.Strings(extra)
	msg := fmt.Sprintf("%s takes %s", name, strings.Join(s.order, ", "))
	if len(missing) > 0 {
		msg += "; missing " + strings.Join(missing, ", ")
	}
	if len(extra) > 0 {
		msg += "; unexpected " + strings.Join(extra, ", ")
	}
	return msg
}

func optionalNames(s Schema) []string {
	names := make([]string, 0, len(s.Optional))
	for f := range s.Optional {
		names = append(names, f)
	}
	sort.Strings(names)
	return names
}

func fits(shape json.RawMessage, value any) bool {
	var allowed []string
	if json.Unmarshal(shape, &allowed) == nil {
		text, ok := value.(string)
		return ok && slices.Contains(allowed, text)
	}
	var kind string
	if err := json.Unmarshal(shape, &kind); err != nil {
		panic(err)
	}
	text, isText := value.(string)
	switch kind {
	case "int":
		n, ok := value.(float64)
		return ok && n == float64(int64(n))
	case "str":
		return isText && strings.TrimSpace(text) != ""
	case "sha":
		return isText && shaText.MatchString(text)
	case "time":
		return isText && timeText.MatchString(text)
	}
	panic("unknown shape " + kind)
}

func describe(shape json.RawMessage) string {
	var allowed []string
	if json.Unmarshal(shape, &allowed) == nil {
		return "one of " + strings.Join(allowed, ", ")
	}
	var kind string
	if err := json.Unmarshal(shape, &kind); err != nil {
		panic(err)
	}
	return map[string]string{"int": "an integer", "str": "non-empty text", "sha": "a full 40-character sha", "time": "a UTC time like 2026-10-31T06:00:00Z"}[kind]
}

func command(name string, fields map[string]any, to []string) string {
	var parts []string
	for _, f := range append(slices.Clone(typed[name].order), optionalNames(typed[name])...) {
		if v, ok := fields[f]; ok {
			parts = append(parts, "--"+f, jsonText(v))
		}
	}
	for _, lane := range to {
		parts = append(parts, "--to", lane)
	}
	return "lr post " + name + " " + strings.Join(parts, " ")
}

func jsonText(v any) string {
	if text, ok := v.(string); ok {
		return text
	}
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}
