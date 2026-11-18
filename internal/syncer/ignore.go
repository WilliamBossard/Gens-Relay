package syncer

import (
	"os"
	"path"
	"path/filepath"
	"strings"
)

type ignoreRule struct {
	pattern string
	negate  bool
	dirOnly bool
}

type IgnoreMatcher struct {
	rules []ignoreRule
}

func LoadIgnoreMatcher(folderRoot string, configPatterns string) *IgnoreMatcher {
	var lines []string
	if b, err := os.ReadFile(filepath.Join(folderRoot, ".gensignore")); err == nil {
		lines = append(lines, strings.Split(string(b), "\n")...)
	}
	if configPatterns != "" {
		lines = append(lines, strings.Split(configPatterns, "\n")...)
	}

	var rules []ignoreRule
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		negate := false
		if strings.HasPrefix(line, "!") {
			negate = true
			line = strings.TrimSpace(line[1:])
		}
		dirOnly := strings.HasSuffix(line, "/")
		line = strings.TrimSuffix(line, "/")
		line = strings.TrimPrefix(line, "./")
		line = filepath.ToSlash(line)
		if line == "" {
			continue
		}
		rules = append(rules, ignoreRule{
			pattern: line,
			negate:  negate,
			dirOnly: dirOnly,
		})
	}
	return &IgnoreMatcher{rules: rules}
}

func IsInternalName(name string) bool {
	lower := strings.ToLower(name)
	if lower == ".gensversions" || lower == ".gensignore" || lower == ".gensfolder" || strings.Contains(lower, ".gens.tmp.") || strings.Contains(lower, ".gens.bak.") {
		return true
	}
	switch lower {
	case ".ds_store", "thumbs.db", "desktop.ini", "ehthumbs.db":
		return true
	}
	return strings.HasPrefix(lower, "._")
}

// IsInternalPath returns true if any segment of fullPath relative to root is an internal name.
func IsInternalPath(root, fullPath string) bool {
	rel, err := filepath.Rel(root, fullPath)
	if err != nil {
		return IsInternalName(filepath.Base(fullPath))
	}
	for _, seg := range strings.Split(filepath.ToSlash(rel), "/") {
		if IsInternalName(seg) {
			return true
		}
	}
	return false
}

// Match returns true if relPath (slash-separated) should be excluded from sync.
func (m *IgnoreMatcher) Match(relPath string, isDir bool) bool {
	relPath = filepath.ToSlash(strings.TrimPrefix(relPath, "./"))
	base := path.Base(relPath)
	if IsInternalName(base) {
		return true
	}
	for _, seg := range strings.Split(relPath, "/") {
		if IsInternalName(seg) {
			return true
		}
	}

	if m == nil || len(m.rules) == 0 {
		return false
	}

	ignored := false
	for _, r := range m.rules {
		if r.dirOnly && !isDir {
			// Also ignore files inside ignored directory
			if strings.HasPrefix(relPath, r.pattern+"/") {
				ignored = !r.negate
			}
			continue
		}
		if matchPattern(r.pattern, relPath, base) {
			ignored = !r.negate
		}
	}
	return ignored
}

func matchPattern(pattern, relPath, base string) bool {
	if !strings.Contains(pattern, "/") {
		if ok, _ := path.Match(pattern, base); ok {
			return true
		}
		for _, seg := range strings.Split(relPath, "/") {
			if ok, _ := path.Match(pattern, seg); ok {
				return true
			}
		}
		return false
	}
	if ok, _ := path.Match(pattern, relPath); ok {
		return true
	}
	if strings.HasPrefix(relPath, pattern+"/") {
		return true
	}
	return false
}
