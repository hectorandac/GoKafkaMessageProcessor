package middlewares

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/mgo.v2"

	"github.com/go-martini/martini"
)

func MongoDB() martini.Handler {
	uri := os.Getenv("MONGODB_URL")

	if uri == "" {
		uri = "mongodb://localhost:27017/kafka_provisioning"
	}

	mInfo := &mgo.DialInfo{
		Addrs:    []string{"localhost:27017"},
		Database: "provisioning",
		Timeout:  60 * time.Second,
	}
	session, err := mgo.DialWithInfo(mInfo)
	if err != nil {
		fmt.Printf("Can't connect to mongo, go error %v\n", err)
		os.Exit(1)
	}
	session.SetSafe(&mgo.Safe{})

	return func(c martini.Context) {
		s := session.Clone()
		c.Map(s.DB(mInfo.Database))
	}
}
