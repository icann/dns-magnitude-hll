package internal

import (
	"strings"
	"testing"
)

func TestGetDomainName(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		numLabels   uint8
		expected    DomainName
		expectError bool
		errorMsg    string
	}{
		// Valid domain names
		{
			name:      "simple domain",
			input:     "example.com",
			numLabels: 1,
			expected:  DomainName("com"),
		},
		{
			name:      "three-label domain with 1 label requested",
			input:     "www.example.com",
			numLabels: 1,
			expected:  DomainName("com"),
		},
		{
			name:      "domain with trailing dot",
			input:     "example.org.",
			numLabels: 1,
			expected:  DomainName("org"),
		},
		{
			name:      "uppercase domain",
			input:     "EXAMPLE.NET",
			numLabels: 1,
			expected:  DomainName("net"),
		},
		{
			name:      "single label",
			input:     "com",
			numLabels: 1,
			expected:  DomainName("com"),
		},
		{
			name:      "root domain",
			input:     ".",
			numLabels: 1,
			expected:  DomainName("."),
		},
		{
			name:      "empty string",
			input:     "",
			numLabels: 1,
			expected:  DomainName("."),
		},
		{
			name:      "internationalized domain (xn--)",
			input:     "example.xn--p1ai",
			numLabels: 1,
			expected:  DomainName("xn--p1ai"),
		},
		{
			name:      "long subdomain chain",
			input:     "a.b.c.d.e.f.example.com",
			numLabels: 1,
			expected:  DomainName("com"),
		},
		{
			name:      "three labels requested, only TLD is validated",
			input:     "1.2.com",
			numLabels: 3,
			expected:  DomainName("1.2.com"),
		},
		{
			name:      "two labels requested",
			input:     "example.com",
			numLabels: 2,
			expected:  DomainName("example.com"),
		},

		// Invalid domain names
		{
			name:        "too few labels for request",
			input:       "com",
			numLabels:   2,
			expectError: true,
			errorMsg:    "domain name has 1 parts but 2 required",
		},
		{
			name:        "numeric TLD",
			input:       "example.123",
			numLabels:   1,
			expectError: true,
			errorMsg:    "invalid domain name: 123 does not match required pattern",
		},
		{
			name:        "TLD with special characters",
			input:       "example.com/",
			numLabels:   1,
			expectError: true,
			errorMsg:    "invalid domain name: com/ does not match required pattern",
		},
		{
			name:        "TLD too short",
			input:       "example.c",
			numLabels:   1,
			expectError: true,
			errorMsg:    "invalid domain name: c does not match required pattern",
		},
		{
			name:        "TLD too long",
			input:       "example." + strings.Repeat("a", 64),
			numLabels:   1,
			expectError: true,
			errorMsg:    "does not match required pattern",
		},
		{
			name:        "TLD with numbers not xn--",
			input:       "example.com1",
			numLabels:   1,
			expectError: true,
			errorMsg:    "invalid domain name: com1 does not match required pattern",
		},
		{
			name:        "invalid xn-- format",
			input:       "example.xn--",
			numLabels:   1,
			expectError: true,
			errorMsg:    "invalid domain name: xn-- does not match required pattern",
		},
		{
			name:        "xn-- with invalid characters",
			input:       "example.xn--test@",
			numLabels:   1,
			expectError: true,
			errorMsg:    "invalid domain name: xn--test@ does not match required pattern",
		},
		{
			name:        "xn-- too long",
			input:       "example.xn--" + strings.Repeat("a", 60),
			numLabels:   1,
			expectError: true,
			errorMsg:    "does not match required pattern",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getDomainName(tt.input, tt.numLabels)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}
