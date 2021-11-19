package utils

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/mgo.v2"
)

func MongoDB(address string, database string) (*mgo.Database, *mgo.Session) {
	mInfo := &mgo.DialInfo{
		Addrs:    []string{address},
		Database: database,
		Timeout:  60 * time.Second,
	}
	session, err := mgo.DialWithInfo(mInfo)
	if err != nil {
		fmt.Printf("Can't connect to mongo, go error %v\n", err)
		os.Exit(1)
	}
	session.SetSafe(&mgo.Safe{})
	return session.DB(mInfo.Database), session
}
