package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// lastLineKey is the sentinel upstream keeps last in the file, the reason why
// every new key lands just before it, on both sides.
const lastLineKey = "meta.last_line"

// entries is a locale file: its keys in file order and their values as raw
// JSON, so a value the merge leaves alone comes out byte for byte.
type entries struct {
	keys []string
	vals map[string]json.RawMessage
}

func parseLocale(name string, data []byte) (*entries, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if tok != json.Delim('{') {
		return nil, fmt.Errorf("%s: not a JSON object", name)
	}
	e := &entries{vals: map[string]json.RawMessage{}}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("%s: unexpected token %v", name, tok)
		}
		if _, dup := e.vals[key]; dup {
			return nil, fmt.Errorf("%s: duplicate key %q", name, key)
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		e.keys = append(e.keys, key)
		e.vals[key] = raw
	}
	if _, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: content after the JSON object", name)
	}
	return e, nil
}

// same reports whether a key has the same value, or is absent, in both files.
func same(a, b *entries, key string) bool {
	va, ina := a.vals[key]
	vb, inb := b.vals[key]
	return ina == inb && bytes.Equal(va, vb)
}

// MergeLocale merges a locale_next JSON file three ways, by key rather than
// by line. Upstream appends its keys at the end of the file, where this fork
// appends its own, so nearly every upstream string conflicts line by line
// although no key does.
//
// A key one side alone changed takes that side's value; a key both sides
// changed differently fails the merge. The keys THEIRS holds come first in
// THEIRS's order and the fork's own follow in OURS's order, so that the diff
// against upstream stays a single block at the end of the file, before the
// sentinel lastLineKey.
func MergeLocale(base, ours, theirs []byte) ([]byte, error) {
	b, err := parseLocale("base", base)
	if err != nil {
		return nil, err
	}
	o, err := parseLocale("ours", ours)
	if err != nil {
		return nil, err
	}
	t, err := parseLocale("theirs", theirs)
	if err != nil {
		return nil, err
	}

	var conflicts []error
	vals := make(map[string]json.RawMessage, len(o.keys)+len(t.keys))
	resolve := func(key string) {
		src := o // THEIRS left it alone, or both sides agree
		switch {
		case same(t, b, key) || same(o, t, key):
		case same(o, b, key):
			src = t
		default:
			conflicts = append(conflicts, fmt.Errorf("locale key %q changed on both sides", key))
			return
		}
		if v, ok := src.vals[key]; ok {
			vals[key] = v
		}
	}
	for _, list := range [][]string{b.keys, o.keys, t.keys} {
		for _, key := range list {
			resolve(key)
		}
	}
	if err := errors.Join(conflicts...); err != nil {
		return nil, err
	}

	var out bytes.Buffer
	out.WriteString("{\n")
	seen := make(map[string]bool, len(vals))
	first := true
	for _, list := range [][]string{t.keys, o.keys, {lastLineKey}} {
		for _, key := range list {
			v, kept := vals[key]
			if !kept || seen[key] || (key == lastLineKey && !isLast(list)) {
				continue
			}
			seen[key] = true
			if !first {
				out.WriteString(",\n")
			}
			first = false
			var name bytes.Buffer
			enc := json.NewEncoder(&name)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(key); err != nil {
				return nil, err
			}
			out.WriteByte('\t')
			out.Write(bytes.TrimSpace(name.Bytes()))
			out.WriteString(": ")
			out.Write(v)
		}
	}
	out.WriteString("\n}\n")
	return out.Bytes(), nil
}

// isLast reports whether list is the final pass of the emit loop, the one
// holding only the sentinel.
func isLast(list []string) bool {
	return len(list) == 1 && list[0] == lastLineKey
}
