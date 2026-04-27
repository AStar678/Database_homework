package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer 词法分析器
type Lexer struct {
	input string
	pos   int // 当前字符位置
	readPos int // 下一个读取位置
	ch    byte // 当前字符
}

// NewLexer 创建词法分析器
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) skipComment() {
	if l.ch == '-' && l.peekChar() == '-' {
		for l.ch != '\n' && l.ch != 0 {
			l.readChar()
		}
		l.skipWhitespace()
	}
}

// NextToken 获取下一个Token
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	l.skipComment()
	l.skipWhitespace()

	var tok Token
	switch l.ch {
	case '(':
		tok = Token{Type: TOKEN_LPAREN, Literal: string(l.ch)}
	case ')':
		tok = Token{Type: TOKEN_RPAREN, Literal: string(l.ch)}
	case ',':
		tok = Token{Type: TOKEN_COMMA, Literal: string(l.ch)}
	case ';':
		tok = Token{Type: TOKEN_SEMICOLON, Literal: string(l.ch)}
	case '*':
		tok = Token{Type: TOKEN_STAR, Literal: string(l.ch)}
	case '=':
		tok = Token{Type: TOKEN_EQ, Literal: string(l.ch)}
	case '.':
		tok = Token{Type: TOKEN_DOT, Literal: string(l.ch)}
	case '<':
		if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TOKEN_NE, Literal: literal}
		} else if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TOKEN_LE, Literal: literal}
		} else {
			tok = Token{Type: TOKEN_LT, Literal: string(l.ch)}
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TOKEN_GE, Literal: literal}
		} else {
			tok = Token{Type: TOKEN_GT, Literal: string(l.ch)}
		}
	case '\'':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString('\'')
	case '"':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString('"')
	case 0:
		tok = Token{Type: TOKEN_EOF, Literal: ""}
	default:
		if isLetter(l.ch) {
			literal := l.readIdentifier()
			tok.Type = LookupIdent(literal)
			tok.Literal = literal
			return tok
		} else if isDigit(l.ch) {
			literal := l.readNumber()
			if strings.Contains(literal, ".") {
				tok.Type = TOKEN_FLOAT
			} else {
				tok.Type = TOKEN_NUMBER
			}
			tok.Literal = literal
			return tok
		} else {
			tok = Token{Type: TOKEN_ILLEGAL, Literal: string(l.ch)}
		}
	}
	l.readChar()
	return tok
}

func (l *Lexer) readIdentifier() string {
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readNumber() string {
	start := l.pos
	for isDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readString(quote byte) string {
	var out strings.Builder
	l.readChar() // skip opening quote
	for l.ch != quote && l.ch != 0 {
		if l.ch == '\\' && l.peekChar() == quote {
			l.readChar()
		}
		out.WriteByte(l.ch)
		l.readChar()
	}
	// l.ch is closing quote or 0
	return out.String()
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_'
}

func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
}

// Tokenize 将输入分词为Token列表
func Tokenize(input string) ([]Token, error) {
	l := NewLexer(input)
	var tokens []Token
	for {
		tok := l.NextToken()
		if tok.Type == TOKEN_ILLEGAL {
			return nil, fmt.Errorf("illegal token: %s", tok.Literal)
		}
		tokens = append(tokens, tok)
		if tok.Type == TOKEN_EOF {
			break
		}
	}
	return tokens, nil
}
