package check_pb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func genAuth(timestamp uint64, locality string, data []byte, authToken []byte) string {
	hash := hmac.New(sha256.New, authToken)
	hash.Write([]byte(locality))
	hash.Write([]byte(strconv.FormatInt(int64(timestamp), 10)))
	if len(data) > 0 {
		hash.Write(data)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func checkAuth(timestamp uint64, locality string, data []byte, auth string, authToken []byte, authValidSec int64) error {
	current := time.Now().Unix()
	ts := int64(timestamp)
	if ts-current > 3 {
		return status.Error(codes.InvalidArgument, "client time error")
	}
	if current-ts > authValidSec {
		return status.Error(codes.InvalidArgument, "timestamp expired")
	}
	hash := hmac.New(sha256.New, []byte(authToken))
	hash.Write([]byte(locality))
	hash.Write([]byte(strconv.FormatInt(int64(timestamp), 10)))
	if len(data) > 0 {
		hash.Write(data)
	}
	if hex.EncodeToString(hash.Sum(nil)) != auth {
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
