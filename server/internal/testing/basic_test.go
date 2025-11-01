package testing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicFunctionality(t *testing.T) {
	// 测试基本的 Go 功能
	t.Run("String operations", func(t *testing.T) {
		str := "Hello, World!"
		assert.Equal(t, "Hello, World!", str)
		assert.Contains(t, str, "World")
		assert.Len(t, str, 13)
	})

	t.Run("Slice operations", func(t *testing.T) {
		slice := []int{1, 2, 3, 4, 5}
		assert.Len(t, slice, 5)
		assert.Equal(t, 1, slice[0])
		assert.Equal(t, 5, slice[4])
	})

	t.Run("Map operations", func(t *testing.T) {
		m := map[string]int{
			"one":   1,
			"two":   2,
			"three": 3,
		}
		assert.Len(t, m, 3)
		assert.Equal(t, 1, m["one"])
		assert.Equal(t, 2, m["two"])
		assert.Equal(t, 3, m["three"])
	})
}

func TestMathOperations(t *testing.T) {
	t.Run("Addition", func(t *testing.T) {
		result := 2 + 3
		assert.Equal(t, 5, result)
	})

	t.Run("Multiplication", func(t *testing.T) {
		result := 4 * 5
		assert.Equal(t, 20, result)
	})

	t.Run("Division", func(t *testing.T) {
		result := 10 / 2
		assert.Equal(t, 5, result)
	})
}

func TestErrorHandling(t *testing.T) {
	t.Run("Error creation", func(t *testing.T) {
		err := assert.AnError
		assert.Error(t, err)
		assert.NotNil(t, err)
	})

	t.Run("No error", func(t *testing.T) {
		var err error
		assert.NoError(t, err)
		assert.Nil(t, err)
	})
}

func TestConditionalLogic(t *testing.T) {
	t.Run("Boolean operations", func(t *testing.T) {
		trueValue := true
		falseValue := false

		assert.True(t, trueValue)
		assert.False(t, falseValue)
		assert.NotEqual(t, trueValue, falseValue)
	})

	t.Run("Comparison operations", func(t *testing.T) {
		a := 10
		b := 20
		c := 10

		assert.Greater(t, b, a)
		assert.Less(t, a, b)
		assert.Equal(t, a, c)
		assert.NotEqual(t, a, b)
	})
}

func TestStructOperations(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	t.Run("Struct creation and access", func(t *testing.T) {
		person := Person{
			Name: "John Doe",
			Age:  30,
		}

		assert.Equal(t, "John Doe", person.Name)
		assert.Equal(t, 30, person.Age)
	})

	t.Run("Struct modification", func(t *testing.T) {
		person := Person{Name: "Jane", Age: 25}
		person.Name = "Jane Smith"
		person.Age = 26

		assert.Equal(t, "Jane Smith", person.Name)
		assert.Equal(t, 26, person.Age)
	})
}

// Writer 接口定义
type Writer interface {
	Write([]byte) (int, error)
}

// StringWriter 实现 Writer 接口
type StringWriter struct {
	content string
}

func (sw *StringWriter) Write(data []byte) (int, error) {
	sw.content += string(data)
	return len(data), nil
}

func TestInterfaceOperations(t *testing.T) {
	t.Run("Interface implementation", func(t *testing.T) {
		var writer Writer = &StringWriter{}

		n, err := writer.Write([]byte("Hello"))
		assert.NoError(t, err)
		assert.Equal(t, 5, n)
	})
}

func TestConcurrencyBasics(t *testing.T) {
	t.Run("Channel operations", func(t *testing.T) {
		ch := make(chan int, 1)

		// 发送数据
		ch <- 42

		// 接收数据
		value := <-ch
		assert.Equal(t, 42, value)
	})

	t.Run("Goroutine basics", func(t *testing.T) {
		done := make(chan bool)

		go func() {
			// 模拟一些工作
			done <- true
		}()

		// 等待完成
		<-done
		assert.True(t, true) // 如果到达这里，说明 goroutine 正常完成
	})
}
