package set

import "github.com/CodingCaius/godis/datastruct/dict"

// Set 是基于哈希表的元素集合
type Set struct {
	dict dict.Dict
}

// Make 创建一个新的集合。它接收可变数量的字符串参数 members
func Make(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimple(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// 向集合中添加一个元素。它接收一个字符串 val 作为参数，并将该字符串添加到 dict 中
func (set *Set) Add(val string) int {
	return set.dict.Put(val, nil)
}

// Remove removes member from set
func (set *Set) Remove(val string) int {
	_, ret := set.dict.Remove(val)
	return ret
}

// Has returns true if the val exists in the set
func (set *Set) Has(val string) bool {
	if set == nil || set.dict == nil {
		return false
	}
	_, exists := set.dict.Get(val)
	return exists
}

// Len returns number of members in the set
func (set *Set) Len() int {
	if set == nil || set.dict == nil {
		return 0
	}
	return set.dict.Len()
}

// ToSlice convert set to []string
func (set *Set) ToSlice() []string {
	slice := make([]string, set.Len())
	i := 0
	set.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			// set extended during traversal
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

// ForEach visits each member in the set
func (set *Set) ForEach(consumer func(member string) bool) {
	if set == nil || set.dict == nil {
		return
	}
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}


// ShallowCopy 将所有成员复制到另一个集合
func (set *Set) ShallowCopy() *Set {
	result := Make()
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

// Intersect 与两个集合相交
func Intersect(sets ...*Set) *Set {
	result := Make()
	if len(sets) == 0 {
		return result
	}

	countMap := make(map[string]int)
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			countMap[member]++
			return true
		})
	}
	for k, v := range countMap {
		if v == len(sets) {
			result.Add(k)
		}
	}
	return result
}

// Union adds two sets
func Union(sets ...*Set) *Set {
	result := Make()
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			result.Add(member)
			return true
		})
	}
	return result
}

// Diff subtracts two sets
func Diff(sets ...*Set) *Set {
	if len(sets) == 0 {
		return Make()
	}
	result := sets[0].ShallowCopy()
	for i := 1; i < len(sets); i++ {
		sets[i].ForEach(func(member string) bool {
			result.Remove(member)
			return true
		})
		if result.Len() == 0 {
			break
		}
	}
	return result
}

// RandomMembers randomly returns keys of the given number, may contain duplicated key
func (set *Set) RandomMembers(limit int) []string {
	if set == nil || set.dict == nil {
		return nil
	}
	return set.dict.RandomKeys(limit)
}

// RandomDistinctMembers randomly returns keys of the given number, won't contain duplicated key
func (set *Set) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}




