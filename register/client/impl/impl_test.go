package impl

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"

	util_bytes "github.com/spolabs/nebula/util/bytes"
)

func (self *ClientRegisterService) encrypt(data []byte) ([]byte, error) {
	return rsa.EncryptPKCS1v15(rand.Reader, self.PubKey, data)
}

func TestDecrypt(t *testing.T) {
	var data, en, plain []byte
	var err error
	crs := NewClientRegisterService()
	data = []byte("test data")
	en, err = crs.encrypt(data)
	if err != nil {
		t.Errorf("Failed.")
	}
	plain, err = crs.decrypt(en)
	if err != nil {
		t.Errorf("Failed.")
	}
	if !util_bytes.SameBytes(data, plain) {
		t.Errorf("Failed.")
	}
}
