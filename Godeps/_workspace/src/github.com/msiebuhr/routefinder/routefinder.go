package routefinder

// Package routefinder implements functions for decomposing known dynamic URLs into templates.

import (
	"fmt"
	"regexp"
	"strings"
)

var converterRegex *regexp.Regexp

func init() {
	var err error
	converterRegex, err = regexp.Compile(":[^/]+")
	if err != nil {
		panic(err)
	}
}

// Routefinder represents an ordered table of URL-like route templates, allowing
// reverse lookup of these into the original template along with parsed-out
// variables.
type Routefinder []struct {
	name    string
	re      regexp.Regexp
	prefix  string
	hasVars bool
}

// NewRoutefinder constructs a Route-table from the given templates. Everything
// in the template from a : to the following / is considered a variable.
func NewRoutefinder(templates ...string) (Routefinder, error) {
	// Regex placeholders out of the template URLs
	routes := make(Routefinder, 0, len(templates))

	for _, template := range templates {
		err := routes.Add(template)
		if err != nil {
			return Routefinder{}, err
		}
	}

	return routes, nil
}

func (r *Routefinder) Add(template string) error {
	if template == "" {
		return nil
	}

	// Quote slashes &c.
	withQuotedMeta := regexp.QuoteMeta(template)

	// Switch out :[^\/]+ for capture groups
	withQuotedMeta = converterRegex.ReplaceAllStringFunc(withQuotedMeta, func(group string) string {
		return fmt.Sprintf("(?P<%s>[^/]+)", group[1:])
	})

	if strings.HasSuffix(withQuotedMeta, "/\\.\\.\\.") {
		withQuotedMeta = withQuotedMeta[:len(withQuotedMeta)-7] + "\\/(?P<__TRAILING__>.*)"
	}

	// Add start and end guards
	withQuotedMeta = fmt.Sprintf("^%s$", withQuotedMeta)

	re, err := regexp.Compile(withQuotedMeta)

	if err != nil {
		return err
	}

	prefix, whole := re.LiteralPrefix()

	*r = append(*r, struct {
		name    string
		re      regexp.Regexp
		prefix  string
		hasVars bool
	}{
		name:    template,
		re:      *re,
		prefix:  prefix,
		hasVars: !whole,
	})

	return nil
}

// Lookup the given path in the available routes, first-match-wins.  A match
// will return the original template string along with a map of the parsed-out
// variables. Lookup returns empty values, if no match is found.
func (r Routefinder) Lookup(path string) (string, map[string]string) {
	// Dump any query string
	normalizedPath := strings.SplitN(path, "?", 1)[0]

	// Check key against regex'es
	for _, template := range r {
		// Get the prefix of the regex and test it
		if !strings.HasPrefix(normalizedPath, template.prefix) {
			continue
		}

		// Is the prefix == the whole thing? Then a match == win
		if !template.hasVars && normalizedPath == template.name {
			return template.name, map[string]string{}
		}

		// Check key against regexes
		if !template.re.MatchString(normalizedPath) {
			continue
		}

		subMatchNames := template.re.SubexpNames()
		subMatchValues := template.re.FindStringSubmatch(normalizedPath)

		meta := make(map[string]string, len(subMatchNames))

		for j := 1; j < len(subMatchNames); j++ {
			meta[subMatchNames[j]] = subMatchValues[j]
		}

		templateName := template.name

		// If the template ends with /... and we extracted __TRAILING__, transplant it back
		if trailing, ok := meta["__TRAILING__"]; ok && strings.HasSuffix(template.name, "/...") {
			templateName = templateName[:len(templateName)-3] + trailing
			delete(meta, "__TRAILING__")
		}

		return templateName, meta
	}

	return "", map[string]string{}
}

func (r Routefinder) String() string {
	strs := make([]string, len(r))

	for i, str := range r {
		strs[i] = str.name
	}

	return strings.Join(strs, ",")
}

// Set appends new routes, by parsing comma-delimited sets of routes. Used to
// implement flags.Value
func (r *Routefinder) Set(in string) error {
	for _, template := range strings.Split(in, ",") {
		if err := r.Add(template); err != nil {
			return err
		}
	}

	return nil
}
