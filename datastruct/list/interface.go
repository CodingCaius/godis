package list

// Expected 检查给定项目是否等于预期值
type Expected func(a interface{}) bool

//Consumer 遍历列表。
//它接收索引和值作为参数，返回 true 继续遍历，返回 false 中断
type Consumer func(i int, v interface{}) bool

type List interface {
	Add(val interface{})
	Get(index int) (val interface{})
	Set(index int, val interface{})
	Insert(index int, val interface{})
	Remove(index int) (val interface{})
	RemoveLast() (val interface{})
	RemoveAllByVal(expected Expected) int
	RemoveByVal(expected Expected, count int) int
	ReverseRemoveByVal(expected Expected, count int) int
	Len() int
	ForEach(consumer Consumer)
	Contains(expected Expected) bool
	Range(start int, stop int) []interface{}
}