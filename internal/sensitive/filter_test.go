package sensitive

import "testing"

func TestFilterMatchesSensitiveWord(t *testing.T) {
	filter := New([]string{" 赌博 ", "SCAM"})

	if word, ok := filter.Match("有人在赌博"); !ok || word != "赌博" {
		t.Fatalf("Match() = %q, %v; want 赌博, true", word, ok)
	}
	if word, ok := filter.Match("this is a scam"); !ok || word != "scam" {
		t.Fatalf("Match() = %q, %v; want scam, true", word, ok)
	}
}

func TestFilterDoesNotMatchCleanContent(t *testing.T) {
	filter := New([]string{"赌博", "诈骗"})

	if word, ok := filter.Match("欢迎来到直播间"); ok || word != "" {
		t.Fatalf("Match() = %q, %v; want empty, false", word, ok)
	}
}

func TestFilterIgnoresEmptyWords(t *testing.T) {
	filter := New([]string{" ", ""})

	if word, ok := filter.Match("ordinary message"); ok || word != "" {
		t.Fatalf("Match() = %q, %v; want empty, false", word, ok)
	}
}
