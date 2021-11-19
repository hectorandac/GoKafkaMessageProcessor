package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	common_models "github.com/hectorandac/kafka-message-processor/common-models"
	"github.com/hectorandac/kafka-message-processor/message-logger/utils"
	"gopkg.in/mgo.v2"
)

var configuration map[string]interface{}

const DATABASE = "logger"
const PROVISIONING_URL = "http://localhost:3010/config/kafka_service_config"

func main() {
	consumerClient, _ := setupEnvironment()
	defer consumerClient.Close()
	consumerClient.SubscribeTopics([]string{"messaging_otp", "messaging_trx", "messaging_cmp"}, nil)

	dbConnection, dbSession := utils.MongoDB(configuration["database_address"].(string), DATABASE)
	defer dbSession.Close()

	for {
		msg, err := consumerClient.ReadMessage(-1)
		if err == nil {
			fmt.Printf("✅ Message on: %s, Date Time: %s\n", *msg.TopicPartition.Topic, time.Now())
			go http.Post("http://0.0.0.0:5555/gelf", "application/json", bytes.NewBuffer(msg.Value))
			go persistInDB(dbConnection, msg.Value)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func persistInDB(dbConnection *mgo.Database, message []byte) {
	var result common_models.Message
	json.Unmarshal(message, &result)
	go dbConnection.C("messages").Insert(result)
}

func setupEnvironment() (*kafka.Consumer, error) {
	body, err := getRequest(PROVISIONING_URL)

	if err != nil {
		return &kafka.Consumer{}, errors.New("couldn't retrieve information form provisioning service")
	}

	if body["status"] == "successful" {

		configuration = body["result"].(map[string]interface{})

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": configuration["kafka_host"],
			"group.id":          "message_reader",
			"auto.offset.reset": "earliest",
		})
		if err != nil {
			panic(err)
		}
		return c, nil
	} else {
		return &kafka.Consumer{}, errors.New("unsuccessful request")
	}
}

func getRequest(url string) (map[string]interface{}, error) {
	resp, err := http.Get(url)
	if err != nil {
		return map[string]interface{}{}, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return map[string]interface{}{}, err
	}

	return toJson(string(body)), err
}

func toJson(jsonString string) map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(jsonString), &result)
	return result
}
