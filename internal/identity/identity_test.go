package identity

import "testing"

func TestMatchesHostAcceptsShortNameAndFQDN(t *testing.T) {
	hosts := []string{"cscf01.es.tz.vimpelcom.ru", "cscf01"}
	for _, instance := range []string{"cscf01", "CSCF01.es.tz.vimpelcom.ru", "cscf01.es.tz.vimpelcom.ru."} {
		if !MatchesHost(instance, hosts) {
			t.Fatalf("instance %q should match %v", instance, hosts)
		}
	}
}

func TestMatchesHostRejectsOtherHost(t *testing.T) {
	hosts := []string{"vm-imsb2c-ts-msk-lab-p-cscf-1"}
	if MatchesHost("cscf01.es.tz.vimpelcom.ru", hosts) {
		t.Fatal("production instance unexpectedly matched the lab hostname")
	}
}

func TestWarningsForMissingAndMismatchedInstance(t *testing.T) {
	hosts := []string{"vm-imsb2c-ts-msk-lab-p-cscf-1"}
	if warnings := Warnings(map[string]string{"site": "msk"}, hosts); len(warnings) != 1 {
		t.Fatalf("missing instance warnings = %v", warnings)
	}
	if warnings := Warnings(map[string]string{"instance": "cscf01.es.tz.vimpelcom.ru"}, hosts); len(warnings) != 1 {
		t.Fatalf("mismatch warnings = %v", warnings)
	}
	if warnings := Warnings(map[string]string{"instance": "vm-imsb2c-ts-msk-lab-p-cscf-1"}, hosts); len(warnings) != 0 {
		t.Fatalf("matching instance warnings = %v", warnings)
	}
}

func TestFormatLabelsSortsNames(t *testing.T) {
	got := FormatLabels(map[string]string{"site": "msk", "instance": "cscf01", "role": "cscf"})
	want := `instance="cscf01" role="cscf" site="msk"`
	if got != want {
		t.Fatalf("FormatLabels = %q, want %q", got, want)
	}
}
