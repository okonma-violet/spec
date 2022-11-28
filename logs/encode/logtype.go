package encode

type LogType byte

const (
	Debug   LogType = 1
	Info    LogType = 2
	Warning LogType = 3
	Error   LogType = 4
)

func (lt LogType) String() string {
	switch lt {
	case Debug:
		return "DBG"
	case Info:
		return "INF"
	case Warning:
		return "WRN"
	case Error:
		return "ERR"
	}
	return "UNK"
}
func (lt LogType) Byte() byte {
	return byte(lt)
}
func (lt LogType) ByteStr() []byte {
	switch lt {
	case Debug:
		return []byte("DBG")
	case Info:
		return []byte("INF")
	case Warning:
		return []byte("WRN")
	case Error:
		return []byte("ERR")
	}
	return []byte("UNK")
}
