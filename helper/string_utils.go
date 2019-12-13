package helper

import "unicode/utf8"

func UnCapitalizeStr(s string) string {

	if len(s) == 0 {
		return s
	}

	isASCII, hasUpper := true, false
	c := s[0]
	if c >= utf8.RuneSelf {
		isASCII = false
	}
	hasUpper = hasUpper || (c >= 'A' && c <= 'Z')

	if isASCII {
		if !hasUpper {
			return s
		}
		b := make([]byte, len(s))
		for i := 0; i < len(s); i++ {
			c := s[i]
			if i == 0 {
				if c >= 'A' && c <= 'Z' {
					c += 'a' - 'A'
				}
			}
			b[i] = c
		}
		return string(b)
	}

	return s
}
