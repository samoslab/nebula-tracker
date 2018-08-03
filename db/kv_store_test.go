package db

import (
	"testing"

	"nebula-tracker/config"
)

func TestKvStore(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	var (
		key          = "testkey"
		intVal int64 = 12312
		strVal       = ""
	)
	saveKvStore(tx, key, intVal, strVal)
	i, s, found := getKvStore(tx, key)
	if !found {
		t.Errorf("Failed. not found")
	}
	if i != intVal {
		t.Errorf("Failed. expect:%d, actual:%d", intVal, i)
	}
	if s != strVal {
		t.Errorf("Failed. expect:%s, actual:%s", strVal, s)
	}
	intVal = 0
	strVal = "testval"
	saveKvStore(tx, key, intVal, strVal)
	i, s, found = getKvStore(tx, key)
	if !found {
		t.Errorf("Failed. not found")
	}

	if i != intVal {
		t.Errorf("Failed. expect:%d, actual:%d", intVal, i)
	}
	if s != strVal {
		t.Errorf("Failed. expect:%s, actual:%s", strVal, s)
	}

	i, s, found = getKvStore(tx, key+"1")
	if found {
		t.Errorf("Failed.  found")
	}

}
