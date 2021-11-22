package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	common_models "github.com/hectorandac/kafka-message-processor/common-models"
)

var consumerClient *kafka.Consumer
var producerClient *kafka.Producer
var configuration map[string]interface{}
var nextSubcriptionTarget string

func main() {
	setupEnvironment()
	consumerClient.SubscribeTopics([]string{nextSubcriptionTarget}, nil)
	fmt.Printf("Registered to: %s\n", nextSubcriptionTarget)
	defer consumerClient.Close()

	for {
		msg, err := consumerClient.ReadMessage(-1)
		if err == nil {
			var result common_models.Message
			json.Unmarshal(msg.Value, &result)
			result.ReceivedOn = time.Now().UnixNano()
			messageProcessor(result)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

// Stubbed function that makes thread sleep for two senconds to mimic the time it would take to send a message to a mobile device
func messageProcessor(message common_models.Message) {
	fmt.Printf("FROM QUEUE [%s] Message processed: %s\n", nextSubcriptionTarget, message.Message)
	produce(message)
	message.ProcessedOn = time.Now().UnixNano()
}

func produce(message common_models.Message) {
	topic := configuration["reporting_queue"].(string)
	result, err := json.Marshal(message)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = producerClient.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: -1},
		Value:          []byte(result),
	}, nil)

	if err != nil {
		fmt.Println(err.Error())
	}

}

func setupEnvironment() error {
	err := setKafkaConfiguration()
	if err != nil {
		return err
	}

	err = setupServerConsumerTarget()
	if err != nil {
		return err
	}

	return nil
}

func setupServerConsumerTarget() error {
	url := "http://0.0.0.0:3000/register_consumer"
	body, err := getRequest(url)

	if err != nil {
		return errors.New("couldn't retrieve information form provisioning service")
	}

	if body["result"] == "registered" {
		nextSubcriptionTarget = body["subscription_target"].(string)
	}

	return err
}

func setKafkaConfiguration() error {
	url := "http://localhost:3010/config/kafka_service_config"
	body, err := getRequest(url)

	if err != nil {
		return errors.New("couldn't retrieve information form provisioning service")
	}

	if body["status"] == "successful" {
		configuration = body["result"].(map[string]interface{})

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": configuration["kafka_host"],
			"group.id":          "message_dispatcher",
			"auto.offset.reset": "earliest",
		})
		if err != nil {
			panic(err)
		}
		consumerClient = c

		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": configuration["kafka_host"]})
		if err != nil {
			panic(err)
		}
		producerClient = p

		return nil
	} else {
		return errors.New("unsuccessful request")
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
