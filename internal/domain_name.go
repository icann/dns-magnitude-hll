// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"regexp"
	"strings"
)

// DomainName represents a normalized domain name (last two labels, lowercased)
type DomainName string

// regex for domain name validation. Pre-compiled for performance.
var domainNameRegex = regexp.MustCompile("^(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$")

// getDomainName lowercases and extracts the last N labels of a domain name
func getDomainName(name string, numLabels uint8) (DomainName, error) {
	if len(name) == 0 || name == "." {
		return DomainName("."), nil
	}

	name = strings.ToLower(name)

	// Remove trailing dot if present
	if name[len(name)-1] == '.' {
		name = name[:len(name)-1]
	}

	split := strings.Split(name, ".")

	// Reject domain names with too few labels
	idx := len(split) - int(numLabels)
	if idx < 0 {
		return DomainName(""), fmt.Errorf("domain name has %d parts but %d required", len(split), numLabels)
	}

	// Validate the TLD using the regex. If "labels" is greater than 1, the caller should validate the rest.
	tld := split[len(split)-1]
	if !domainNameRegex.MatchString(tld) {
		return DomainName(""), fmt.Errorf("invalid domain name: %s does not match required pattern", tld)
	}

	// Join the desired number of labels with "."
	res := strings.Join(split[idx:], ".")
	return DomainName(res), nil
}
