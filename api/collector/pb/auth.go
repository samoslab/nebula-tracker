package forcollector_pb

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"time"

	util_bytes "github.com/samoslab/nebula/util/bytes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func genAuth(timestamp uint64, nodeId string, data []byte, authToken []byte) []byte {
	hash := hmac.New(sha256.New, authToken)
	hash.Write(util_bytes.FromUint64(timestamp))
	if len(nodeId) > 0 {
		hash.Write([]byte(nodeId))
	}
	if len(data) > 0 {
		hash.Write(data)
	}
	return hash.Sum(nil)
}

func checkAuth(timestamp uint64, nodeId string, data []byte, auth []byte, authToken []byte, authValidSec int64) error {
	current := time.Now().Unix()
	ts := int64(timestamp)
	if ts-current > 3 {
		return status.Error(codes.InvalidArgument, "client time error")
	}
	if current-ts > authValidSec {
		return status.Error(codes.InvalidArgument, "timestamp expired")
	}
	if !bytes.Equal(genAuth(timestamp, nodeId, data, authToken), auth) {
		return status.Error(codes.Unauthenticated, "auth verify error")
	}
	return nil
}

func (self *ClientPubKeyReq) GenAuth(authToken []byte) {
	self.Auth = genAuth(self.Timestamp, self.NodeId, nil, authToken)
}

func (self *ClientPubKeyReq) CheckAuth(authToken []byte, authValidSec int64) error {
	return checkAuth(self.Timestamp, self.NodeId, nil, self.Auth, authToken, authValidSec)
}

func (self *ProviderPubKeyReq) GenAuth(authToken []byte) {
	self.Auth = genAuth(self.Timestamp, self.NodeId, nil, authToken)
}

func (self *ProviderPubKeyReq) CheckAuth(authToken []byte, authValidSec int64) error {
	return checkAuth(self.Timestamp, self.NodeId, nil, self.Auth, authToken, authValidSec)
}

func (self *HourlyUpdateReq) GenAuth(authToken []byte) {
	self.Auth = genAuth(self.Timestamp, "", self.Data, authToken)
}

func (self *HourlyUpdateReq) CheckAuth(authToken []byte, authValidSec int64) error {
	return checkAuth(self.Timestamp, "", self.Data, self.Auth, authToken, authValidSec)
}

func (self *GetLastSummaryReq) GenAuth(authToken []byte) {
	self.Auth = genAuth(self.Timestamp, "", nil, authToken)
}

func (self *GetLastSummaryReq) CheckAuth(authToken []byte, authValidSec int64) error {
	return checkAuth(self.Timestamp, "", nil, self.Auth, authToken, authValidSec)
}
