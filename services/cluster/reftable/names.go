// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package reftable

import (
	"errors"
	"path"
	"strings"
	"unicode/utf8"
)

// The validators below are pure Go and pinned to the payload version
// (ADR 0152 D19, R1-06): a git binary of another version on another node can
// never change Apply's verdict, and nothing asks the filesystem.

// ValidRefName applies the rules of git check-ref-format, without
// --allow-onelevel, plus the one symbolic name the table holds at the top
// level, HEAD. Version 1 of the payload.
//
// A name must also be valid UTF-8, which git does not require: the entries
// are JSON, whose encoder replaces an invalid byte with U+FFFD, so the
// table would hold another name than git's, and two names would become
// one. The rule changes no verdict of Apply on any entry, whatever its
// version: encoding/json decodes every string as valid UTF-8. It refuses
// such a name before it is proposed.
func ValidRefName(name string) error {
	if name == "HEAD" {
		return nil
	}
	if !utf8.ValidString(name) {
		return errors.New("ref name is not valid UTF-8")
	}
	if !strings.HasPrefix(name, "refs/") {
		return errors.New("ref outside refs/")
	}
	if len(name) > 1024 {
		return errors.New("ref name too long")
	}
	if strings.HasSuffix(name, "/") || strings.Contains(name, "//") {
		return errors.New("empty ref name component")
	}
	if strings.HasSuffix(name, ".") {
		return errors.New("ref name ends with a dot")
	}
	if strings.Contains(name, "..") {
		return errors.New("ref name contains two dots in a row")
	}
	if strings.Contains(name, "@{") || name == "@" {
		return errors.New("ref name contains @{")
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c < 0x20 || c == 0x7f {
			return errors.New("ref name contains a control character")
		}
		switch c {
		case ' ', '~', '^', ':', '?', '*', '[', '\\':
			return errors.New("ref name contains a forbidden character")
		}
	}
	for comp := range strings.SplitSeq(name, "/") {
		if comp == "" {
			return errors.New("empty ref name component")
		}
		if comp[0] == '.' {
			return errors.New("ref name component starts with a dot")
		}
		if strings.HasSuffix(comp, ".lock") {
			return errors.New("ref name component ends with .lock")
		}
	}
	return nil
}

// ValidRepoKey checks a repository key lexically: "owner/name.git", both
// components lowercase as Forgejo stores them on disk, nothing that could
// escape the repository root. Payload versions 1 to 4 refuse a name that
// starts with a dot; a key that only GenerationVersion admits passes too,
// so this is the check of a key the table already holds, or of a path.
func ValidRepoKey(key string) error {
	return ValidRepoKeyAt(key, PayloadVersion)
}

// ValidRepoKeyAt is ValidRepoKey as Apply checks it at payload version v.
// Forgejo accepts a repository name that starts with a dot, ".profile" the
// owner's profile among them, and refuses "." and ".." only: from
// GenerationVersion the name may too (#54). The owner never starts with a
// dot, which keeps the projector's ".cluster-stale" outside every key.
func ValidRepoKeyAt(key string, v uint8) error {
	if key == "" || len(key) > 512 {
		return errors.New("repository key empty or too long")
	}
	if path.Clean(key) != key || path.IsAbs(key) {
		return errors.New("repository key is not a clean relative path")
	}
	parts := strings.Split(key, "/")
	if len(parts) != 2 || !strings.HasSuffix(parts[1], ".git") || len(parts[1]) == len(".git") {
		return errors.New("repository key must be owner/name.git")
	}
	for i, p := range parts {
		if name := strings.TrimSuffix(p, ".git"); i == 1 && v >= GenerationVersion && name != "." && name != ".." {
			// The name: a leading dot is Forgejo's, "." and ".." are not.
		} else if p == "." || p == ".." || strings.HasPrefix(p, ".") {
			return errors.New("repository key component starts with a dot")
		}
		for i := 0; i < len(p); i++ {
			c := p[i]
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
				return errors.New("repository key holds a character Forgejo does not write")
			}
		}
	}
	return nil
}

// validSHA accepts a lowercase hex object id of the given format.
func validSHA(sha, format string) bool {
	n := 40
	if format == "sha256" {
		n = 64
	}
	if len(sha) != n {
		return false
	}
	for i := 0; i < len(sha); i++ {
		c := sha[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// IsZeroSHA reports whether sha is git's null object id of either format.
func IsZeroSHA(sha string) bool {
	return sha != "" && strings.Trim(sha, "0") == ""
}
