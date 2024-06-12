package sortedset

import (
	"errors"
	"strconv"
)

/*
* ScoreBorder 是一个结构体，表示 redis 命令 `ZRANGEBYSCORE` 的 `min` `max` 参数
* 可以接受：
* int 或 float 值，例如 2.718, 2, -2.718, -2 ...
* 独占 int 或 float 值，例如 (2.718, (2, (-2.718, (-2 ...
* 无穷大： +inf, -inf， inf(与 +inf 相同)
 */

// 这些常量用于表示边界的正无穷和负无穷值，以及词典序的边界
const (
	scoreNegativeInf int8 = -1
	scorePositiveInf int8 = 1
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)


// 用于比较边界值、获取边界值及其排他性，并检查边界是否相交
type Border interface {
	greater(element *Element) bool
	less(element *Element) bool
	getValue() interface{}
	getExclude() bool
	isIntersected(max Border) bool
}

// 用于描述数值范围
// ScoreBorder represents range of a float value, including: <, <=, >, >=, +inf, -inf
type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

//用于判断元素的分数是否在上边界内
// if max.greater(score) then the score is within the upper border
// do not use min.greater()
func (border *ScoreBorder) greater(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return false
	} else if border.Inf == scorePositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

//判断元素的分数是否在下边界内
func (border *ScoreBorder) less(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return true
	} else if border.Inf == scorePositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

// 返回边界的值
func (border *ScoreBorder) getValue() interface{} {
	return border.Value
}

// 返回边界的排他性
func (border *ScoreBorder) getExclude() bool {
	return border.Exclude
}

var scorePositiveInfBorder = &ScoreBorder{
	Inf: scorePositiveInf,
}

var scoreNegativeInfBorder = &ScoreBorder{
	Inf: scoreNegativeInf,
}

// 根据字符串创建 ScoreBorder 对象，支持正无穷、负无穷、排他性边界和普通浮点数边界
// ParseScoreBorder creates ScoreBorder from redis arguments
func ParseScoreBorder(s string) (Border, error) {
	if s == "inf" || s == "+inf" {
		return scorePositiveInfBorder, nil
	}
	if s == "-inf" {
		return scoreNegativeInfBorder, nil
	}
	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}

// 判断两个边界是否相交
func (border *ScoreBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*ScoreBorder).Value
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

// 描述字符串范围
// LexBorder represents range of a string value, including: <, <=, >, >=, +, -
type LexBorder struct {
	Inf     int8
	Value   string
	Exclude bool
}

// 判断元素的成员是否在上边界内
// if max.greater(lex) then the lex is within the upper border
// do not use min.greater()
func (border *LexBorder) greater(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return false
	} else if border.Inf == lexPositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

// 判断元素的成员是否在下边界内
func (border *LexBorder) less(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return true
	} else if border.Inf == lexPositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

// 返回边界的值
func (border *LexBorder) getValue() interface{} {
	return border.Value
}

func (border *LexBorder) getExclude() bool {
	return border.Exclude
}

var lexPositiveInfBorder = &LexBorder{
	Inf: lexPositiveInf,
}

var lexNegativeInfBorder = &LexBorder{
	Inf: lexNegativeInf,
}

// 根据字符串创建 LexBorder 对象，支持正无穷、负无穷、排他性边界和普通字符串边界
// ParseLexBorder creates LexBorder from redis arguments
func ParseLexBorder(s string) (Border, error) {
	if s == "+" {
		return lexPositiveInfBorder, nil
	}
	if s == "-" {
		return lexNegativeInfBorder, nil
	}
	if s[0] == '(' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: true,
		}, nil
	}

	if s[0] == '[' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: false,
		}, nil
	}

	return nil, errors.New("ERR min or max not valid string range item")
}

func (border *LexBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*LexBorder).Value
	return border.Inf == '+' || minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}
