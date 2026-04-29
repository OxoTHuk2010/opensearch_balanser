package simulator

import "testing"

func TestSummarizeConflicts(t *testing.T) {
	conflicts := []string{
		"allocator check failed for a/0: ... low watermark ...",
		"allocator check failed for b/1: some other error",
		"allocator denied i/1",
		"step x/2 breaches watermark on n1",
		"step y/3 missing source shard on n2",
		"unexpected",
	}
	got := summarizeConflicts(conflicts)
	if got["allocator_low_watermark"] != 1 {
		t.Fatalf("expected allocator_low_watermark=1, got %d", got["allocator_low_watermark"])
	}
	if got["allocator_check_failed"] != 1 {
		t.Fatalf("expected allocator_check_failed=1, got %d", got["allocator_check_failed"])
	}
	if got["allocator_denied"] != 1 {
		t.Fatalf("expected allocator_denied=1, got %d", got["allocator_denied"])
	}
	if got["simulated_watermark_breach"] != 1 {
		t.Fatalf("expected simulated_watermark_breach=1, got %d", got["simulated_watermark_breach"])
	}
	if got["missing_source_shard"] != 1 {
		t.Fatalf("expected missing_source_shard=1, got %d", got["missing_source_shard"])
	}
	if got["other"] != 1 {
		t.Fatalf("expected other=1, got %d", got["other"])
	}
}

