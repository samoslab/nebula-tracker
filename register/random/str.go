package random

import (
	math_rand "math/rand"
	"time"
)

var mathRand *math_rand.Rand

func init() {
	mathRand = math_rand.New(math_rand.NewSource(time.Now().UnixNano()))
}

func RandomStr(strlen int) string {
	const chars = "abcdefghijkmnpqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := range result {
		result[i] = chars[mathRand.Intn(len(chars))]
	}
	return string(result)
}
