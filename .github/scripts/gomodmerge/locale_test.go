package main

import (
	"strings"
	"testing"
)

const localeBase = `{
	"a": "A",
	"plural": {
		"one": "%d fork",
		"other": "%d forks"
	},
	"edited": "old",
	"gone": "bye",
	"last": "L"
}
`

// Both sides append after "last", which git reports as a conflict.
const localeOurs = `{
	"a": "A",
	"plural": {
		"one": "%d fork",
		"other": "%d forks"
	},
	"edited": "old",
	"gone": "bye",
	"last": "L",
	"admin.cluster": "Cluster",
	"admin.cluster.node": "Node <b>"
}
`

const localeTheirs = `{
	"a": "A",
	"inserted": "I",
	"plural": {
		"one": "%d fork",
		"other": "%d forks"
	},
	"edited": "new",
	"last": "L",
	"upstream.added": "U"
}
`

const localeWant = `{
	"a": "A",
	"inserted": "I",
	"plural": {
		"one": "%d fork",
		"other": "%d forks"
	},
	"edited": "new",
	"last": "L",
	"upstream.added": "U",
	"admin.cluster": "Cluster",
	"admin.cluster.node": "Node <b>"
}
`

func TestMergeLocaleKeepsBothAppends(t *testing.T) {
	got, err := MergeLocale([]byte(localeBase), []byte(localeOurs), []byte(localeTheirs))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != localeWant {
		t.Fatalf("got:\n%s\nwant:\n%s", got, localeWant)
	}
}

func TestMergeLocaleKeepsSentinelLast(t *testing.T) {
	sentinel := func(block string) string {
		return strings.Replace(block, "\n}\n", ",\n\t\"meta.last_line\": \"M\"\n}\n", 1)
	}
	got, err := MergeLocale([]byte(sentinel(localeBase)), []byte(sentinel(localeOurs)), []byte(sentinel(localeTheirs)))
	if err != nil {
		t.Fatal(err)
	}
	if want := sentinel(localeWant); string(got) != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestMergeLocaleUnchangedRoundTrips(t *testing.T) {
	got, err := MergeLocale([]byte(localeBase), []byte(localeBase), []byte(localeBase))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != localeBase {
		t.Fatalf("got:\n%s\nwant:\n%s", got, localeBase)
	}
}

func TestMergeLocaleSameChangeBothSides(t *testing.T) {
	both := strings.Replace(localeBase, `"old"`, `"new"`, 1)
	got, err := MergeLocale([]byte(localeBase), []byte(both), []byte(both))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != both {
		t.Fatalf("got:\n%s\nwant:\n%s", got, both)
	}
}

func TestMergeLocaleConflicts(t *testing.T) {
	for name, tc := range map[string]struct{ ours, theirs string }{
		"edited both sides": {
			strings.Replace(localeBase, `"old"`, `"mine"`, 1),
			strings.Replace(localeBase, `"old"`, `"theirs"`, 1),
		},
		"added both sides": {
			strings.Replace(localeBase, `"last": "L"`, `"last": "L", "x": "1"`, 1),
			strings.Replace(localeBase, `"last": "L"`, `"last": "L", "x": "2"`, 1),
		},
		"dropped upstream, edited here": {
			strings.Replace(localeBase, `"bye"`, `"mine"`, 1),
			strings.Replace(localeBase, "\t\"gone\": \"bye\",\n", "", 1),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := MergeLocale([]byte(localeBase), []byte(tc.ours), []byte(tc.theirs)); err == nil {
				t.Fatal("want a conflict")
			}
		})
	}
}

func TestMergeLocaleRefusesBadInput(t *testing.T) {
	for name, in := range map[string]string{
		"not an object": `["a"]`,
		"duplicate key": `{"a": "1", "a": "2"}`,
		"trailing data": `{"a": "1"} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := MergeLocale([]byte(in), []byte(localeBase), []byte(localeBase)); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}
