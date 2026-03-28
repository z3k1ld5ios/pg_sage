package llm

import "strings"

// StripSQLComments removes block comments (/* ... */, including
// nested) and line comments (-- ...) from SQL text. Comments
// inside single-quoted string literals are preserved.
func StripSQLComments(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	i := 0
	for i < len(text) {
		// Single-quoted string literal: copy verbatim.
		if text[i] == '\'' {
			b.WriteByte(text[i])
			i++
			for i < len(text) {
				b.WriteByte(text[i])
				if text[i] == '\'' {
					// Escaped quote '' inside string.
					if i+1 < len(text) && text[i+1] == '\'' {
						b.WriteByte(text[i+1])
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}

		// Block comment: skip (handle nesting).
		if i+1 < len(text) &&
			text[i] == '/' && text[i+1] == '*' {
			i = skipBlockComment(text, i)
			b.WriteByte(' ') // replace comment with space
			continue
		}

		// Line comment: skip to end of line.
		if i+1 < len(text) &&
			text[i] == '-' && text[i+1] == '-' {
			i += 2
			for i < len(text) && text[i] != '\n' {
				i++
			}
			continue
		}

		b.WriteByte(text[i])
		i++
	}
	return b.String()
}

// skipBlockComment advances past a /* ... */ block comment,
// handling nesting. Returns the index after the closing */.
func skipBlockComment(text string, start int) int {
	depth := 0
	i := start
	for i < len(text) {
		if i+1 < len(text) &&
			text[i] == '/' && text[i+1] == '*' {
			depth++
			i += 2
			continue
		}
		if i+1 < len(text) &&
			text[i] == '*' && text[i+1] == '/' {
			depth--
			i += 2
			if depth == 0 {
				return i
			}
			continue
		}
		i++
	}
	return i // unterminated comment: skip to end
}
