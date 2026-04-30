package query

import (
	"testing"
)

func TestParse_BareTerm(t *testing.T) {
	node, err := Parse("error")
	if err != nil {
		t.Fatal(err)
	}
	n, ok := node.(TermNode)
	if !ok {
		t.Fatalf("expected TermNode, got %T", node)
	}
	if n.Token != "error" {
		t.Fatalf("want error, got %s", n.Token)
	}
}

func TestParse_FieldFilter(t *testing.T) {
	node, err := Parse("level:error")
	if err != nil {
		t.Fatal(err)
	}
	n, ok := node.(FieldNode)
	if !ok {
		t.Fatalf("expected FieldNode, got %T", node)
	}
	if n.Field != "level" || n.Value != "error" {
		t.Fatalf("want level:error, got %s:%s", n.Field, n.Value)
	}
}

func TestParse_And(t *testing.T) {
	node, err := Parse("error AND timeout")
	if err != nil {
		t.Fatal(err)
	}
	n, ok := node.(AndNode)
	if !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
	if _, ok := n.Left.(TermNode); !ok {
		t.Fatalf("expected TermNode left, got %T", n.Left)
	}
}

func TestParse_Or(t *testing.T) {
	node, err := Parse("error OR timeout")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := node.(OrNode); !ok {
		t.Fatalf("expected OrNode, got %T", node)
	}
}

func TestParse_AndBindsTighter(t *testing.T) {
	// "a OR b AND c" should parse as "a OR (b AND c)"
	node, err := Parse("a OR b AND c")
	if err != nil {
		t.Fatal(err)
	}
	or, ok := node.(OrNode)
	if !ok {
		t.Fatalf("top node should be OrNode, got %T", node)
	}
	if _, ok := or.Right.(AndNode); !ok {
		t.Fatalf("right of OR should be AndNode, got %T", or.Right)
	}
}

func TestParse_Grouping(t *testing.T) {
	node, err := Parse("(level:error OR level:warn) AND service:api")
	if err != nil {
		t.Fatal(err)
	}
	and, ok := node.(AndNode)
	if !ok {
		t.Fatalf("expected AndNode at root, got %T", node)
	}
	if _, ok := and.Left.(OrNode); !ok {
		t.Fatalf("expected OrNode left, got %T", and.Left)
	}
}

func TestParse_Empty(t *testing.T) {
	node, err := Parse("")
	if err != nil {
		t.Fatal(err)
	}
	if node != nil {
		t.Fatalf("empty string should return nil node, got %T", node)
	}
}

func TestParse_MalformedMissingOperand(t *testing.T) {
	_, err := Parse("AND error")
	if err == nil {
		t.Fatal("expected error for malformed input")
	}
}
