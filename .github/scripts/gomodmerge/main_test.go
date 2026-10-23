package main

import (
	"strings"
	"testing"
)

const base = `module forgejo.org

go 1.26.0

toolchain go1.27.2

require (
	example.com/a v1.0.0
	example.com/b v1.0.0
	example.com/gone v1.0.0
	example.com/c v1.0.0 // indirect
)

replace example.com/old => example.com/fork v1.0.0
`

// ours: the fork's shape, with its own require, replace and comment.
const ours = `module forgejo.org

go 1.27.3

require (
	example.com/a v1.0.0
	example.com/b v1.2.0
	example.com/gone v1.0.0
	example.com/sdk v0.1.0
	example.com/c v1.0.0 // indirect
)

replace example.com/old => example.com/fork v1.0.0

// Kept as written by the fork.
replace example.com/sdk => example.com/sdk v0.0.0-20261023084429-60c961201ab2
`

// theirs: upstream's bumps next to the fork's lines, and its go directive.
const theirs = `module forgejo.org

go 1.26.0

toolchain go1.27.3

require (
	example.com/a v1.1.0
	example.com/b v1.1.0
	example.com/new v0.3.0
	example.com/c v1.4.0 // indirect
)

replace example.com/old => example.com/fork v1.1.0
`

const want = `module forgejo.org

go 1.27.3

require (
	example.com/a v1.1.0
	example.com/b v1.2.0
	example.com/sdk v0.1.0
	example.com/c v1.4.0 // indirect
	example.com/new v0.3.0
)

replace example.com/old => example.com/fork v1.1.0

// Kept as written by the fork.
replace example.com/sdk => example.com/sdk v0.0.0-20261023084429-60c961201ab2
`

func TestMerge(t *testing.T) {
	got, err := Merge([]byte(base), []byte(ours), []byte(theirs))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestMergeKeepsToolchainAboveGo(t *testing.T) {
	o := strings.Replace(ours, "go 1.27.3", "go 1.26.0", 1)
	got, err := Merge([]byte(base), []byte(o), []byte(theirs))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "go 1.26.0\n\ntoolchain go1.27.3\n") {
		t.Fatalf("upstream's toolchain bump lost:\n%s", got)
	}
}

func TestMergeReplaceChangedOnBothSides(t *testing.T) {
	o := strings.Replace(ours, "example.com/fork v1.0.0", "example.com/fork v1.0.5", 1)
	if _, err := Merge([]byte(base), []byte(o), []byte(theirs)); err == nil ||
		!strings.Contains(err.Error(), "example.com/old") {
		t.Fatalf("want a conflict on example.com/old, got %v", err)
	}
}

func TestMergeUnparsable(t *testing.T) {
	if _, err := Merge([]byte(base), []byte("<<<<<<< HEAD\n"), []byte(theirs)); err == nil {
		t.Fatal("want a parse error")
	}
}
