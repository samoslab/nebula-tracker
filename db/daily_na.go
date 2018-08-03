package db

import (
	"database/sql"
	"time"
)

func saveDailyNa(tx *sql.Tx, providerId string, day string, naSection [][2]int64) {
	if len(naSection) == 0 {
		return
	}
	stmt, err := tx.Prepare("INSERT INTO DAILY_NA (CREATION,PROVIDER_ID,DAY,START_TIME,END_TIME) VALUES (now(),$1,$2,$3,$4)")
	defer stmt.Close()
	checkErr(err)
	for _, arr := range naSection {
		_, err = stmt.Exec(providerId, day, time.Unix(arr[0], 0), time.Unix(arr[1], 0))
		checkErr(err)
	}
}

func getDailyNaByProviderAndDay(tx *sql.Tx, providerId string, day string) (naSection [][2]time.Time) {
	rows, err := tx.Query("SELECT START_TIME,END_TIME FROM DAILY_NA where PROVIDER_ID=$1 and DAY=$2 order by START_TIME asc", providerId, day)
	checkErr(err)
	defer rows.Close()
	res := make([][2]time.Time, 0, 4)
	for rows.Next() {
		var s, e time.Time
		err = rows.Scan(&s, &e)
		checkErr(err)
		res = append(res, [2]time.Time{s, e})
	}
	return res
}

func GetDailyNaByProviderAndDay(providerId string, day string) (naSection [][2]time.Time) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	naSection = getDailyNaByProviderAndDay(tx, providerId, day)
	checkErr(tx.Commit())
	commit = true
	return
}

const KEY_LAST_START string = "dailysummarize-lastStart"

func DailyNaSummarize(naThreshold int64, offset int64) {
	i, _, found := GetKvStore(KEY_LAST_START)
	if !found {
		i = 1532822400 //2018/7/29 00:00:00 UTC
	}
	current := time.Now().Unix() - 86400 - 300
	for ; i < current; i += 86400 {
		dailySummarize(i, naThreshold, offset)
	}
}

func dailySummarize(start int64, naThreshold int64, offset int64) {
	ps := GetProviderOfCheckAvailRecord(time.Unix(start, 0), time.Unix(start+86400, 0))
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	day := time.Unix(start, 0).UTC().Format("2006-01-02")
	for _, pid := range ps {
		checkTimeSlice := getByProviderAndCheckTimeBetween(tx, pid, time.Unix(start-naThreshold, 0), time.Unix(start+86400+naThreshold, 0))
		naSection := getNaSection(start, checkTimeSlice, naThreshold, offset)
		saveDailyNa(tx, pid, day, naSection)
	}
	saveKvStore(tx, KEY_LAST_START, start+86400, "")
	checkErr(tx.Commit())
	commit = true
}

func getNaSection(dayStart int64, checks []int64, naThreshold int64, offset int64) [][2]int64 {
	last := dayStart
	var next int64 = 0
	dayEnd := dayStart + 86400
	res := make([][2]int64, 0, 32)
	for _, check := range checks {
		if check >= dayStart {
			if check >= dayEnd {
				next = check
				break
			}
			if check-last > naThreshold {
				if last == dayStart {
					res = append(res, [2]int64{last, check - offset})
				} else {
					res = append(res, [2]int64{last + offset, check - offset})
				}
			}
		}
		last = check
	}
	if last < dayEnd-offset {
		if next > 0 {
			if next-last > naThreshold {
				res = append(res, [2]int64{last + offset, dayEnd})
			}
		} else {
			res = append(res, [2]int64{last + offset, dayEnd})
		}
	}
	return res
}
