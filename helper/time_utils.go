package helper

import "time"

func CurrentTimeMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
