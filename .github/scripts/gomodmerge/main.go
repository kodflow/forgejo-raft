// Command gomodmerge resolves a go.mod merge conflict three ways, for the
// Sync upstream workflow: git merges go.mod line by line, so an upstream
// version bump next to a line this fork added, or an edited go directive,
// stops the whole sync although the two sides do not disagree.
//
//	gomodmerge BASE OURS THEIRS > go.mod
//
// It starts from OURS, so this fork's lines and their comments stay as
// written, and applies what THEIRS changed since BASE:
//   - a requirement THEIRS added or moved is taken, at the higher of the two
//     versions when OURS moved it too;
//   - a requirement THEIRS dropped is dropped, unless OURS moved it;
//   - a replace THEIRS added, changed or dropped is applied, unless OURS
//     changed the same one differently: that is a real conflict, and the
//     command fails;
//   - the go and toolchain directives take the higher version when both
//     sides moved them, and a toolchain no newer than go is dropped;
//   - a change THEIRS made to any other directive (exclude, retract, tool,
//     godebug, ignore) is not merged: the command fails.
//
// The result still has to go through go mod tidy, which also rebuilds go.sum.
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: gomodmerge BASE OURS THEIRS")
		os.Exit(2)
	}
	var data [3][]byte
	for i, name := range os.Args[1:] {
		b, err := os.ReadFile(name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		data[i] = b
	}
	out, err := Merge(data[0], data[1], data[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "gomodmerge:", err)
		os.Exit(1)
	}
	os.Stdout.Write(out)
}

// Merge returns OURS with the changes THEIRS made since BASE.
func Merge(base, ours, theirs []byte) ([]byte, error) {
	b, err := modfile.Parse("base/go.mod", base, nil)
	if err != nil {
		return nil, err
	}
	o, err := modfile.Parse("ours/go.mod", ours, nil)
	if err != nil {
		return nil, err
	}
	t, err := modfile.Parse("theirs/go.mod", theirs, nil)
	if err != nil {
		return nil, err
	}
	if others(t) != others(b) {
		return nil, errors.New("upstream changed an exclude, retract, tool, godebug or ignore directive: merge by hand")
	}
	if err := mergeGo(b, o, t); err != nil {
		return nil, err
	}
	if err := mergeRequires(b, o, t); err != nil {
		return nil, err
	}
	if err := mergeReplaces(b, o, t); err != nil {
		return nil, err
	}
	o.Cleanup()
	return modfile.Format(o.Syntax), nil
}

// others renders the directives Merge does not handle, in file order.
func others(f *modfile.File) string {
	var s strings.Builder
	for _, e := range f.Exclude {
		fmt.Fprintln(&s, "exclude", e.Mod)
	}
	for _, r := range f.Retract {
		fmt.Fprintln(&s, "retract", r.Low, r.High, r.Rationale)
	}
	for _, t := range f.Tool {
		fmt.Fprintln(&s, "tool", t.Path)
	}
	for _, g := range f.Godebug {
		fmt.Fprintln(&s, "godebug", g.Key, g.Value)
	}
	for _, i := range f.Ignore {
		fmt.Fprintln(&s, "ignore", i.Path)
	}
	return s.String()
}

func goVersion(f *modfile.File) string {
	if f.Go == nil {
		return ""
	}
	return f.Go.Version
}

func toolchain(f *modfile.File) string {
	if f.Toolchain == nil {
		return ""
	}
	return f.Toolchain.Name
}

// goSemver maps a Go version (1.27.3) or toolchain name (go1.27.3) to semver.
func goSemver(v string) (string, error) {
	s := "v" + strings.TrimPrefix(v, "go")
	if !semver.IsValid(s) {
		return "", fmt.Errorf("go version %q is not plain MAJOR.MINOR.PATCH: merge by hand", v)
	}
	return s, nil
}

// higherGo returns the higher of two Go versions, an empty one being lowest.
func higherGo(a, b string) (string, error) {
	if a == "" || b == "" {
		return a + b, nil
	}
	sa, err := goSemver(a)
	if err != nil {
		return "", err
	}
	sb, err := goSemver(b)
	if err != nil {
		return "", err
	}
	if semver.Compare(sa, sb) >= 0 {
		return a, nil
	}
	return b, nil
}

func mergeGo(b, o, t *modfile.File) error {
	g := goVersion(o)
	if goVersion(t) != goVersion(b) {
		var err error
		if g == goVersion(b) {
			g = goVersion(t)
		} else if g, err = higherGo(g, goVersion(t)); err != nil {
			return err
		}
	}
	tc := toolchain(o)
	if toolchain(t) != toolchain(b) {
		var err error
		if tc == toolchain(b) {
			tc = toolchain(t)
		} else if tc, err = higherGo(tc, toolchain(t)); err != nil {
			return err
		}
	}
	if g != goVersion(o) {
		if err := o.AddGoStmt(g); err != nil {
			return err
		}
	}
	if tc != "" && g != "" {
		hi, err := higherGo("go"+g, tc)
		if err != nil {
			return err
		}
		if hi == "go"+g {
			tc = ""
		}
	}
	if tc == "" {
		o.DropToolchainStmt()
	} else if tc != toolchain(o) {
		return o.AddToolchainStmt(tc)
	}
	return nil
}

func requires(f *modfile.File) map[string]*modfile.Require {
	m := make(map[string]*modfile.Require, len(f.Require))
	for _, r := range f.Require {
		m[r.Mod.Path] = r
	}
	return m
}

func mergeRequires(b, o, t *modfile.File) error {
	br, or, tr := requires(b), requires(o), requires(t)
	for path, r := range tr {
		if old, ok := br[path]; ok && old.Mod.Version == r.Mod.Version {
			continue // THEIRS left it alone
		}
		mine, ok := or[path]
		if !ok {
			if _, inBase := br[path]; inBase {
				continue // OURS dropped it: tidy puts it back if needed
			}
			o.AddNewRequire(path, r.Mod.Version, r.Indirect)
			continue
		}
		v := r.Mod.Version
		if semver.Compare(mine.Mod.Version, v) > 0 {
			v = mine.Mod.Version
		}
		if v != mine.Mod.Version {
			if err := o.AddRequire(path, v); err != nil {
				return err
			}
		}
	}
	for path, old := range br {
		if _, ok := tr[path]; ok {
			continue
		}
		if mine, ok := or[path]; ok && mine.Mod.Version == old.Mod.Version {
			if err := o.DropRequire(path); err != nil {
				return err
			}
		}
	}
	return nil
}

func replaces(f *modfile.File) map[module.Version]module.Version {
	m := make(map[module.Version]module.Version, len(f.Replace))
	for _, r := range f.Replace {
		m[r.Old] = r.New
	}
	return m
}

func mergeReplaces(b, o, t *modfile.File) error {
	bp, op, tp := replaces(b), replaces(o), replaces(t)
	var conflicts []error
	for old, nw := range tp {
		was, inBase := bp[old]
		if inBase && was == nw {
			continue
		}
		mine, inOurs := op[old]
		if inOurs && mine == nw {
			continue
		}
		if inOurs != inBase || (inOurs && mine != was) {
			conflicts = append(conflicts, fmt.Errorf("replace %s changed on both sides", old))
			continue
		}
		if err := o.AddReplace(old.Path, old.Version, nw.Path, nw.Version); err != nil {
			return err
		}
	}
	for old, was := range bp {
		if _, ok := tp[old]; ok {
			continue
		}
		mine, inOurs := op[old]
		if !inOurs {
			continue
		}
		if mine != was {
			conflicts = append(conflicts, fmt.Errorf("replace %s dropped upstream, changed here", old))
			continue
		}
		if err := o.DropReplace(old.Path, old.Version); err != nil {
			return err
		}
	}
	return errors.Join(conflicts...)
}
