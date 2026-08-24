package model

import "testing"

// The kernel exposes a hyphenated boot id while the journal's _BOOT_ID field is
// not hyphenated. Comparing them verbatim made every restart after a journal
// recovery look like a host reboot.
func TestCanonicalBootIDMatchesKernelAndJournalSpellings(t *testing.T) {
	kernel := "4b1c2f0e-9a3d-4c5b-8e7f-1a2b3c4d5e6f"
	journal := "4b1c2f0e9a3d4c5b8e7f1a2b3c4d5e6f"

	if CanonicalBootID(kernel) != CanonicalBootID(journal) {
		t.Fatalf("kernel %q and journal %q boot ids must be equivalent", CanonicalBootID(kernel), CanonicalBootID(journal))
	}
}

func TestCanonicalBootIDDistinguishesDifferentBoots(t *testing.T) {
	first := CanonicalBootID("4b1c2f0e-9a3d-4c5b-8e7f-1a2b3c4d5e6f")
	second := CanonicalBootID("00000000-9a3d-4c5b-8e7f-1a2b3c4d5e6f")

	if first == second {
		t.Fatal("different boots must not normalize to the same identifier")
	}
}
