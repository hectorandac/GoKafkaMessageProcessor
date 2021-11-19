package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-martini/martini"
	"github.com/go-playground/validator"
	common_models "github.com/hectorandac/kafka-message-processor/common-models"
	"github.com/hectorandac/kafka-message-processor/message-producer/middlewares"
	"github.com/hectorandac/kafka-message-processor/message-producer/models"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/render"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/mgo.v2"
)

var validate *validator.Validate
var producerClient *kafka.Producer
var routings map[string][]string = map[string][]string{}

func main() {
	validate = validator.New()
	setupEnvironment()

	m := martini.Classic()
	m.Use(middlewares.MongoDB())
	m.Use(render.Renderer())

	m.Post("/message", binding.Bind(common_models.Message{}), processMessage)

	m.RunOnAddr(":3020")
}

func processMessage(message common_models.Message, r render.Render, db *mgo.Database) {
	validationError := validate.Struct(message)
	message.CreatedOn = time.Now().UnixNano()

	if validationError != nil {
		json := map[string]interface{}{"error": strings.Split(validationError.Error(), "\n")}
		r.JSON(400, json)
		return
	}

	// produce message
	go produce(message)

	r.JSON(200, map[string]interface{}{"result": "success", "message": message})
}

func produce(message common_models.Message) {
	topics := routings[message.Type]
	result, err := json.Marshal(message)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for _, topic := range topics {
		err = producerClient.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: -1},
			Value:          []byte(result),
		}, nil)

		if err != nil {
			fmt.Println(err.Error())
		}

	}
}

func setupEnvironment() (bool, error) {
	url := "http://localhost:3010/config/kafka_service_config"
	body, err := getRequest(url)

	if err != nil {
		return false, errors.New("couldn't retrieve information form provisioning service")
	}

	if body["status"] == "successful" {
		configuration := body["result"].(map[string]interface{})

		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": configuration["kafka_host"]})
		if err != nil {
			panic(err)
		}

		producerClient = p

		routing_config := configuration["routing"].([]interface{})
		for _, element := range routing_config {
			routing_map := element.(map[string]interface{})

			routing := &models.Routing{}
			mapstructure.Decode(routing_map, &routing)
			routings[routing.Context] = routing.Targets
		}
	}

	return true, nil
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
