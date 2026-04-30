package query

import (
	"fmt"
	"strings"
)

// Node is the interface implemented by all AST node types.
type Node interface{ node() }

// AndNode represents a logical AND of two sub-expressions.
type AndNode struct{ Left, Right Node }

// OrNode represents a logical OR of two sub-expressions.
type OrNode struct{ Left, Right Node }

// TermNode represents a bare keyword term matched against the log message.
type TermNode struct{ Token string }

// FieldNode represents a field:value filter (e.g. level:error).
type FieldNode struct{ Field, Value string }

func (AndNode) node()  {}
func (OrNode) node()   {}
func (TermNode) node() {}
func (FieldNode) node() {}

// Parse parses a boolean query string into a Node AST.
// Returns nil, nil for an empty query string.
// Grammar:
//
//	query    = or_expr
//	or_expr  = and_expr ( "OR" and_expr )*
//	and_expr = atom ( "AND" atom )*
//	atom     = "(" or_expr ")" | field_term | bare_term
//	field_term = IDENT ":" IDENT
//	bare_term  = IDENT
func Parse(query string) (Node, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, nil
	}
	p := &queryParser{tokens: lex(query)}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.tokens) {
		return nil, fmt.Errorf("unexpected token %q at position %d", p.tokens[p.pos], p.pos)
	}
	return node, nil
}

type queryParser struct {
	tokens []string
	pos    int
}

func (p *queryParser) peek() string {
	if p.pos >= len(p.tokens) {
		return ""
	}
	return p.tokens[p.pos]
}

func (p *queryParser) consume() string {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

func (p *queryParser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek() == "OR" {
		p.consume()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = OrNode{Left: left, Right: right}
	}
	return left, nil
}

func (p *queryParser) parseAnd() (Node, error) {
	left, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	for p.peek() == "AND" {
		p.consume()
		right, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		left = AndNode{Left: left, Right: right}
	}
	return left, nil
}

func (p *queryParser) parseAtom() (Node, error) {
	tok := p.peek()
	if tok == "" {
		return nil, fmt.Errorf("unexpected end of query")
	}
	if tok == "AND" || tok == "OR" {
		return nil, fmt.Errorf("unexpected operator %q", tok)
	}
	if tok == "(" {
		p.consume()
		node, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek() != ")" {
			return nil, fmt.Errorf("expected closing ')'")
		}
		p.consume()
		return node, nil
	}
	p.consume()
	if strings.Contains(tok, ":") {
		parts := strings.SplitN(tok, ":", 2)
		return FieldNode{Field: parts[0], Value: parts[1]}, nil
	}
	return TermNode{Token: strings.ToLower(tok)}, nil
}

// lex splits a query string into tokens, treating parentheses as standalone tokens.
func lex(query string) []string {
	var tokens []string
	query = strings.ReplaceAll(query, "(", " ( ")
	query = strings.ReplaceAll(query, ")", " ) ")
	for _, f := range strings.Fields(query) {
		if f != "" {
			tokens = append(tokens, f)
		}
	}
	return tokens
}
