package storage

import "strings"

/*
* Gomap 是一个简单的 map[string]string 的封装
* 主要用于存储键值对数据
* 提供了基本的操作方法，如 Put、Get、Del、Prefix、Suffix 和 Contains
* 这些方法允许用户对存储的数据进行增、查、删和筛选操作
 */
type Gomap map[string]string

func NewGomap(initCap int) *Gomap {
	gomap := Gomap(make(map[string]string, initCap))
	return &gomap
}

func (m *Gomap) Put(key, value string) {
	(*m)[key] = value
}

func (m *Gomap) Get(key string) (string, bool) {
	v, b := (*m)[key]
	return v, b
}

func (m *Gomap) Del(key string) {
	delete(*m, key)
}

func (m *Gomap) Prefix(prefix string) []string {
	result := make([]string, 0)
	for k := range *m {
		if k == "" {
			continue
		}
		if strings.HasPrefix(k, prefix) {
			result = append(result, k)
		}
	}
	return result
}
func (m *Gomap) Suffix(suffix string) []string {
	result := make([]string, 0)
	for k := range *m {
		if k == "" {
			continue
		}
		if strings.HasSuffix(k, suffix) {
			result = append(result, k)
		}
	}
	return result

}
func (m *Gomap) Contains(sub string) []string {
	result := make([]string, 0)
	for k := range *m {
		if k == "" {
			continue
		}
		if strings.Contains(k, sub) {
			result = append(result, k)
		}
	}
	return result
}

// func (m *Gomap) Gomaptest() {
// 	m := NewGomap(10)
// 	m.Put("user:123", "Alice")
// 	m.Put("user:124", "Bob")
// 	m.Put("admin:001", "Charlie")

// 	fmt.Println(m.Prefix("user:"))    // ["user:123" "user:124"]
// 	fmt.Println(m.Suffix("001"))      // ["admin:001"]
// 	fmt.Println(m.Contains("user"))   // ["user:123" "user:124"]
// }
