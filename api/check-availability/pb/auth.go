package check_pb

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"time"

	util_bytes "github.com/samoslab/nebula/util/bytes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func genAuth(timestamp uint64, locality string, data []byte, authToken []byte) []byte {
	hash := hmac.New(sha256.New, authToken)
	hash.Write(util_bytes.FromUint64(timestamp))
	hash.Write([]byte(locality))
	if len(data) > 0 {
		hash.Write(data)
	}
	return hash.Sum(nil)
}

func checkAuth(timestamp uint64, locality string, data []byte, auth []byte, authToken []byte, authValidSec int64) error {
	current := time.Now().Unix()
	ts := int64(timestamp)
	if ts-current > 3 {
		return status.Error(codes.InvalidArgument, "client time error")
	}
	if current-ts > authValidSec {
		return status.Error(codes.InvalidArgument, "timestamp expired")
	}

	if bytes.Equal(genAuth(timestamp, locality, data, authToken), auth) {
		return status.Error(codes.Unauthenticated, "auth verify error")
	}
	return nil
}

func (self *FindProviderReq) GenAuth(authToken []byte) {
	self.Auth = genAuth(self.Timestamp, self.Locality, nil, authToken)
}

func (self *FindProviderReq) CheckAuth(authToken []byte, authValidSec int64) error {
	return checkAuth(self.Timestamp, self.Locality, nil, self.Auth, authToken, authValidSec)
}

func (self *UpdateStatusReq) GenAuth(authToken []byte) {
	self.Auth = genAuth(self.Timestamp, self.Locality, self.Data, authToken)
}

func (self *UpdateStatusReq) CheckAuth(authToken []byte, authValidSec int64) error {
	return checkAuth(self.Timestamp, self.Locality, self.Data, self.Auth, authToken, authValidSec)
}
