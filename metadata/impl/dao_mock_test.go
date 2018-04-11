// Code generated by mockery v1.0.0
package impl

import db "nebula-tracker/db"

import mock "github.com/stretchr/testify/mock"
import rsa "crypto/rsa"

// daoMock is an autogenerated mock type for the daoMock type
type daoMock struct {
	mock.Mock
}

// ClientGetPubKey provides a mock function with given fields: nodeId
func (_m *daoMock) ClientGetPubKey(nodeId []byte) *rsa.PublicKey {
	ret := _m.Called(nodeId)

	var r0 *rsa.PublicKey
	if rf, ok := ret.Get(0).(func([]byte) *rsa.PublicKey); ok {
		r0 = rf(nodeId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*rsa.PublicKey)
		}
	}

	return r0
}

// FileCheckExist provides a mock function with given fields: hash
func (_m *daoMock) FileCheckExist(hash string) (bool, bool, bool, bool, uint64) {
	ret := _m.Called(hash)

	var r0 bool
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(hash)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(string) bool); ok {
		r1 = rf(hash)
	} else {
		r1 = ret.Get(1).(bool)
	}

	var r2 bool
	if rf, ok := ret.Get(2).(func(string) bool); ok {
		r2 = rf(hash)
	} else {
		r2 = ret.Get(2).(bool)
	}

	var r3 bool
	if rf, ok := ret.Get(3).(func(string) bool); ok {
		r3 = rf(hash)
	} else {
		r3 = ret.Get(3).(bool)
	}

	var r4 uint64
	if rf, ok := ret.Get(4).(func(string) uint64); ok {
		r4 = rf(hash)
	} else {
		r4 = ret.Get(4).(uint64)
	}

	return r0, r1, r2, r3, r4
}

// FileOwnerCheckId provides a mock function with given fields: id
func (_m *daoMock) FileOwnerCheckId(id []byte) (string, bool) {
	ret := _m.Called(id)

	var r0 string
	if rf, ok := ret.Get(0).(func([]byte) string); ok {
		r0 = rf(id)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func([]byte) bool); ok {
		r1 = rf(id)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// FileOwnerFileExists provides a mock function with given fields: nodeId, parent, name
func (_m *daoMock) FileOwnerFileExists(nodeId string, parent []byte, name string) ([]byte, bool) {
	ret := _m.Called(nodeId, parent, name)

	var r0 []byte
	if rf, ok := ret.Get(0).(func(string, []byte, string) []byte); ok {
		r0 = rf(nodeId, parent, name)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(string, []byte, string) bool); ok {
		r1 = rf(nodeId, parent, name)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// FileOwnerIdOfFilePath provides a mock function with given fields: nodeId, path
func (_m *daoMock) FileOwnerIdOfFilePath(nodeId string, path string) (bool, []byte) {
	ret := _m.Called(nodeId, path)

	var r0 bool
	if rf, ok := ret.Get(0).(func(string, string) bool); ok {
		r0 = rf(nodeId, path)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 []byte
	if rf, ok := ret.Get(1).(func(string, string) []byte); ok {
		r1 = rf(nodeId, path)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]byte)
		}
	}

	return r0, r1
}

// FileOwnerListOfPath provides a mock function with given fields: nodeId, parentId, pageSize, pageNum, sortField, asc
func (_m *daoMock) FileOwnerListOfPath(nodeId string, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (uint32, []*db.Fof) {
	ret := _m.Called(nodeId, parentId, pageSize, pageNum, sortField, asc)

	var r0 uint32
	if rf, ok := ret.Get(0).(func(string, []byte, uint32, uint32, string, bool) uint32); ok {
		r0 = rf(nodeId, parentId, pageSize, pageNum, sortField, asc)
	} else {
		r0 = ret.Get(0).(uint32)
	}

	var r1 []*db.Fof
	if rf, ok := ret.Get(1).(func(string, []byte, uint32, uint32, string, bool) []*db.Fof); ok {
		r1 = rf(nodeId, parentId, pageSize, pageNum, sortField, asc)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]*db.Fof)
		}
	}

	return r0, r1
}

// FileOwnerMkFolders provides a mock function with given fields: nodeId, parent, folders
func (_m *daoMock) FileOwnerMkFolders(nodeId string, parent []byte, folders []string) string {
	ret := _m.Called(nodeId, parent, folders)

	var r0 string
	if rf, ok := ret.Get(0).(func(string, []byte, []string) string); ok {
		r0 = rf(nodeId, parent, folders)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// FileOwnerRemove provides a mock function with given fields: nodeId, pathId, recursive
func (_m *daoMock) FileOwnerRemove(nodeId string, pathId []byte, recursive bool) bool {
	ret := _m.Called(nodeId, pathId, recursive)

	var r0 bool
	if rf, ok := ret.Get(0).(func(string, []byte, bool) bool); ok {
		r0 = rf(nodeId, pathId, recursive)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// FileRetrieve provides a mock function with given fields: hash
func (_m *daoMock) FileRetrieve(hash string) (bool, bool, []byte, int, []string, uint64) {
	ret := _m.Called(hash)

	var r0 bool
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(hash)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(string) bool); ok {
		r1 = rf(hash)
	} else {
		r1 = ret.Get(1).(bool)
	}

	var r2 []byte
	if rf, ok := ret.Get(2).(func(string) []byte); ok {
		r2 = rf(hash)
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).([]byte)
		}
	}

	var r3 int
	if rf, ok := ret.Get(3).(func(string) int); ok {
		r3 = rf(hash)
	} else {
		r3 = ret.Get(3).(int)
	}

	var r4 []string
	if rf, ok := ret.Get(4).(func(string) []string); ok {
		r4 = rf(hash)
	} else {
		if ret.Get(4) != nil {
			r4 = ret.Get(4).([]string)
		}
	}

	var r5 uint64
	if rf, ok := ret.Get(5).(func(string) uint64); ok {
		r5 = rf(hash)
	} else {
		r5 = ret.Get(5).(uint64)
	}

	return r0, r1, r2, r3, r4, r5
}

// FileReuse provides a mock function with given fields: nodeId, hash, name, size, modTime, parentId
func (_m *daoMock) FileReuse(nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte) {
	_m.Called(nodeId, hash, name, size, modTime, parentId)
}

// FileSaveDone provides a mock function with given fields: nodeId, hash, name, size, modTime, parentId, partitionCount, blocks, storeVolume
func (_m *daoMock) FileSaveDone(nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte, partitionCount int, blocks []string, storeVolume uint64) {
	_m.Called(nodeId, hash, name, size, modTime, parentId, partitionCount, blocks, storeVolume)
}

// FileSaveStep1 provides a mock function with given fields: nodeId, hash, size, storeVolume
func (_m *daoMock) FileSaveStep1(nodeId string, hash string, size uint64, storeVolume uint64) {
	_m.Called(nodeId, hash, size, storeVolume)
}

// FileSaveTiny provides a mock function with given fields: nodeId, hash, fileData, name, size, modTime, parentId
func (_m *daoMock) FileSaveTiny(nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, parentId []byte) {
	_m.Called(nodeId, hash, fileData, name, size, modTime, parentId)
}

// ProviderFindOne provides a mock function with given fields: nodeId
func (_m *daoMock) ProviderFindOne(nodeId string) *db.ProviderInfo {
	ret := _m.Called(nodeId)

	var r0 *db.ProviderInfo
	if rf, ok := ret.Get(0).(func(string) *db.ProviderInfo); ok {
		r0 = rf(nodeId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*db.ProviderInfo)
		}
	}

	return r0
}
