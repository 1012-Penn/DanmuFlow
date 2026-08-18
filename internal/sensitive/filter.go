// Package sensitive 提供弹幕敏感词过滤能力。
package sensitive

import "strings"

// Filter 使用内存中的敏感词列表检查弹幕内容。
// words 在创建时完成清洗，之后只读，因此可以被多个 WebSocket 连接并发调用。
type Filter struct {
	words []string
}

// New 创建一个敏感词过滤器。
// 空白词会被忽略，英文词会统一转换为小写；调用方传入的切片不会被修改。
func New(words []string) *Filter {
	cleaned := make([]string, 0, len(words))
	for _, word := range words {
		word = normalize(word)
		if word == "" {
			continue
		}
		cleaned = append(cleaned, word)
	}

	return &Filter{words: cleaned}
}

// Match 返回弹幕中命中的第一个敏感词。
// 当前版本使用逐词子串匹配，未命中时返回空字符串和 false。
func (f *Filter) Match(content string) (string, bool) {
	content = normalize(content)
	for _, word := range f.words {
		if strings.Contains(content, word) {
			return word, true
		}
	}
	return "", false
}

func normalize(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
