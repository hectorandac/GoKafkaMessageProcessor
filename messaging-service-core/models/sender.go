package models

import "gopkg.in/mgo.v2/bson"

type Sender struct {
	Id        bson.ObjectId `json:"_id,omitempty" bson:"_id,omitempty"`
	Name      string        `json:"name" form:"name" binding:"required" bson:"name"`
	Validated bool          `json:"validated" form:"validated" binding:"required" bson:"validated"`
	CreatedOn int64         `json:"created_on" bson:"created_on"`
}
