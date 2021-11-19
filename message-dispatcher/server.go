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

func main() {
	setupEnvironment()
	consumerClient.SubscribeTopics([]string{"messaging_otp", "messaging_trx", "messaging_cmp"}, nil)
	defer consumerClient.Close()

	otpChannel := make(chan common_models.Message, 200)
	trxChannel := make(chan common_models.Message, 200)
	cmpChannel := make(chan common_models.Message, 200)
	go processChannels(otpChannel, trxChannel, cmpChannel)

	for {
		msg, err := consumerClient.ReadMessage(-1)
		if err == nil {
			var result common_models.Message
			json.Unmarshal(msg.Value, &result)
			result.ReceivedOn = time.Now().UnixNano()

			go assignToChannel(otpChannel, trxChannel, cmpChannel, result)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func assignToChannel(otpChannel chan common_models.Message, trxChannel chan common_models.Message, cmpChannel chan common_models.Message, result common_models.Message) {
	switch result.Type {
	case common_models.OneTimePassword:
		otpChannel <- result
	case common_models.Transactional:
		trxChannel <- result
	case common_models.Campaing:
		cmpChannel <- result
	}
}

func processChannels(otpChannel chan common_models.Message, trxChannel chan common_models.Message, cmpChannel chan common_models.Message) {
	for {
		if len(otpChannel) > 0 {
			message := <-otpChannel
			fmt.Printf("1️⃣ Message processed: %s -> Missing: %d\n", message.Message, len(otpChannel))
			messageProcessor(message)
		} else if len(trxChannel) > 0 {
			message := <-trxChannel
			fmt.Printf("2️⃣ Message processed: %s -> Missing: %d\n", message.Message, len(trxChannel))
			messageProcessor(message)
		} else if len(cmpChannel) > 0 {
			message := <-cmpChannel
			fmt.Printf("3️⃣ Message processed: %s -> Missing: %d\n", message.Message, len(cmpChannel))
			messageProcessor(message)
		}
		// Polling every half a second
		time.Sleep(500 * time.Millisecond)
	}
}

// Stubbed function that makes thread sleep for two senconds to mimic the time it would take to send a message to a mobile device
func messageProcessor(message common_models.Message) {
	message.ProcessedOn = time.Now().UnixNano()
	produce(message)
	time.Sleep(10 * time.Millisecond)
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
