package models

import (
	"gopkg.in/mgo.v2/bson"
)

type ConfigurationDefinition struct {
	Id        bson.ObjectId `json:"_id,omitempty" bson:"_id,omitempty"`
	Key       string        `json:"key" form:"key" binding:"required" bson:"key"`
	Value     string        `json:"value" form:"value" binding:"required" bson:"value"`
	CreatedOn int64         `json:"created_on" bson:"created_on"`
}
