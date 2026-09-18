package identity

import (
	"os"
	"sort"
	"strings"
)

const instanceLabel = "instance"

// LocalHostnames returns the names this process may legitimately use as
// remote_write.labels.instance: os.Hostname, its short form, and /etc/hostname
// when that file exists. Values are lower-cased and de-duplicated.
func LocalHostnames() []string {
	var names []string
	if host, err := os.Hostname(); err == nil {
		names = appendHostname(names, host)
	}
	if body, err := os.ReadFile("/etc/hostname"); err == nil {
		names = appendHostname(names, string(body))
	}
	return names
}

// PrimaryHostname is os.Hostname when it is available, otherwise the first
// remaining alias. An empty list becomes "unknown" so the identity metric
// still has a stable label.
func PrimaryHostname(names []string) string {
	if len(names) == 0 {
		return "unknown"
	}
	return names[0]
}

// Warnings describes copied or incomplete identity labels. The caller logs
// them and continues: a mismatch must not prevent the exporter from starting.
func Warnings(labels map[string]string, hostnames []string) []string {
	instance := strings.TrimSpace(labels[instanceLabel])
	if instance == "" {
		return []string{"remote_write identity: label instance is not set; two exporters with the remaining labels write the same systemd_service_state series"}
	}
	if MatchesHost(instance, hostnames) {
		return nil
	}
	return []string{"remote_write identity: configured instance=" + quote(instance) + " does not match this host (" + strings.Join(hostnames, ", ") + "); a second exporter with the same labels will overwrite systemd_service_state"}
}

// MatchesHost reports whether instance is this host. A short hostname matches
// its FQDN and the other way around, compared without regard to case.
func MatchesHost(instance string, hostnames []string) bool {
	want := normalizeHostname(instance)
	if want == "" {
		return false
	}
	for _, host := range hostnames {
		got := normalizeHostname(host)
		if got == "" {
			continue
		}
		if want == got || strings.HasPrefix(want, got+".") || strings.HasPrefix(got, want+".") {
			return true
		}
	}
	return false
}

// FormatLabels renders configured Remote Write labels for the startup log,
// with names sorted so the line is stable across process restarts.
func FormatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "(none)"
	}
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+quote(labels[name]))
	}
	return strings.Join(parts, " ")
}

func appendHostname(names []string, raw string) []string {
	host := normalizeHostname(raw)
	if host == "" || host == "localhost" {
		return names
	}
	names = appendUnique(names, host)
	if i := strings.IndexByte(host, '.'); i > 0 {
		names = appendUnique(names, host[:i])
	}
	return names
}

func appendUnique(names []string, name string) []string {
	for _, existing := range names {
		if existing == name {
			return names
		}
	}
	return append(names, name)
}

func normalizeHostname(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimSuffix(s, ".")
	if i := strings.IndexAny(s, " \t\n\r"); i >= 0 {
		s = s[:i]
	}
	return s
}

func quote(s string) string {
	return `"` + s + `"`
}
