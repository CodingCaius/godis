package parser

// redis 协议解析器
// 协议解析器将实现TCP服务器的 Handler 接口，充当应用层服务器

// 将接收 Socket 传来的数据，并将其数据还原为 [][]byte 格式，
// 如 "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\value\r\n" 将被还原为 ['SET', 'key', 'value']。

type Payload struct {
	Data redis.Reply
	Err error
}


// ParseStream reads data from io.Reader and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

