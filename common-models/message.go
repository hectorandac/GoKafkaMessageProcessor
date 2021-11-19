package common_models

import "gopkg.in/mgo.v2/bson"

const (
	Campaing        = "CMP"
	Transactional   = "TRX"
	OneTimePassword = "OTP"
)

type Message struct {
	Id          bson.ObjectId `json:"_id,omitempty" bson:"_id,omitempty"`
	Recipient   string        `json:"recipient" form:"recipient" binding:"required" bson:"recipient"`
	Message     string        `json:"message" form:"message" binding:"required" bson:"message"`
	Sender      string        `json:"sender" form:"sender" binding:"required" bson:"sender"`
	Type        string        `json:"type" form:"type" binding:"required" bson:"type" validate:"required,oneof=CMP TRX OTP"`
	CreatedOn   int64         `json:"created_on" bson:"created_on"`
	ReceivedOn  int64         `json:"received_on" bson:"received_on"`
	ProcessedOn int64         `json:"processed_on" bson:"processed_on"`
}
