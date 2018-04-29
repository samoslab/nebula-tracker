package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func random(min int, max int) []byte {
	l := rand.Intn(max - min)
	byteSlice := make([]byte, l+min)
	rand.Read(byteSlice)
	return byteSlice
}

func main() {

	command := flag.NewFlagSet("run", flag.ExitOnError)
	boltDbFlag := command.Bool("b", false, "store to boltdb")
	delRecordFlag := command.Bool("d", false, "delete records")
	countFlag := command.Int("count", 1000, "record count")
	folderFlag := command.String("folder", "db", "db folder name")

	command.Parse(os.Args[1:])

	count := *countFlag

	var d db
	if *boltDbFlag {
		d = &boltDb{}
	} else {
		d = &levelDb{}
	}
	if err := d.open(*folderFlag); err != nil {
		fmt.Println(err)
		return
	}
	defer d.close()
	lstfile, err := os.Create(*folderFlag + ".keylist")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer lstfile.Close()
	start := time.Now().UnixNano()
	for i := 0; i < count; i++ {
		bs := random(32, 512)
		key := sha1Sum(bs)
		_, err := lstfile.Write(key)
		if err != nil {
			fmt.Println(err)
			return
		}
		err = d.put(key, bs)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	fmt.Printf("save count: %d, cost time: %d\n", count, time.Now().UnixNano()-start)
	if !*delRecordFlag {
		return
	}
	lstfile.Seek(0, 0)
	buf := make([]byte, 20)
	count = 0
	start = time.Now().UnixNano()
	for {
		readLen, err := lstfile.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err)
			return
		}
		if readLen == 0 {
			break
		}
		if err = d.del(buf); err != nil {
			fmt.Println(err)
			return
		}
		count++
	}
	fmt.Printf("remove count: %d, cost time: %d\n", count, time.Now().UnixNano()-start)
	lstfile.Seek(0, 0)
	for {
		readLen, err := lstfile.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err)
			return
		}
		if readLen == 0 {
			break
		}
		val, err := d.get(buf)
		if err != nil {
			fmt.Println("get error:" + err.Error())
			return
		}
		if len(val) > 0 {
			fmt.Println(val)
			return
		}
	}
}

func sha1Sum(data []byte) []byte {
	h := sha1.New()
	h.Write(data)
	return h.Sum(nil)
}

type db interface {
	open(path string) error
	close()
	get(key []byte) (value []byte, err error)
	put(key []byte, value []byte) error
	del(key []byte) error
}

type levelDb struct {
	db *leveldb.DB
}

func (self *levelDb) open(path string) (err error) {
	self.db, err = leveldb.OpenFile(path, nil)
	return
}

func (self *levelDb) close() {
	self.db.Close()
}

func (self *levelDb) get(key []byte) (value []byte, err error) {
	value, err = self.db.Get(key, nil)
	if err == leveldb_errors.ErrNotFound {
		return nil, nil
	}
	return
}

func (self *levelDb) put(key []byte, value []byte) error {
	return self.db.Put(key, value, nil)
}

func (self *levelDb) del(key []byte) error {
	return self.db.Delete(key, nil)
}

type boltDb struct {
	db *bolt.DB
}

var bolt_bucket = []byte("default_bucket")

func (self *boltDb) open(path string) (err error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}
	self.db, err = bolt.Open(path+"/my.db", 0600, nil)
	if err != nil {
		return
	}
	err = self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bolt_bucket)
		if b == nil {
			_, er := tx.CreateBucket(bolt_bucket)
			if er != nil {
				return er
			}
		}
		return nil
	})
	return
}

func (self *boltDb) close() {
	self.db.Close()
}

func (self *boltDb) get(key []byte) (value []byte, err error) {
	err = self.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bolt_bucket)
		value = b.Get(key)
		return nil
	})
	return
}

func (self *boltDb) put(key []byte, value []byte) (err error) {
	err = self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bolt_bucket)
		er := b.Put(key, value)
		return er
	})
	return
}

func (self *boltDb) del(key []byte) (err error) {
	err = self.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bolt_bucket)
		er := b.Delete(key)
		return er
	})
	return
}
