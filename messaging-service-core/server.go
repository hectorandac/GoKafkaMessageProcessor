package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-martini/martini"
	common_models "github.com/hectorandac/kafka-message-processor/common-models"
	"github.com/hectorandac/kafka-message-processor/messaging-service-core/middlewares"
	"github.com/hectorandac/kafka-message-processor/messaging-service-core/models"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/render"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var configuration map[string]interface{}
var kafkaAdminClient *kafka.AdminClient

var processDuration int64 = 0
var processedMessages int = 0

var messagesPerSecond map[string]int64 = make(map[string]int64)
var registeredConsumers map[string]int64 = make(map[string]int64)
var queuesPriorities map[string]float64 = make(map[string]float64)

func main() {

	m := martini.Classic()
	m.Use(middlewares.MongoDB())
	m.Use(render.Renderer())

	setupEnvironment()

	fmt.Println(registeredConsumers)
	fmt.Println(queuesPriorities)

	go consumeReporting()

	m.Get("/health", health)
	m.Patch("/core/reconfigure", reconfigure)
	m.Post("/sender/register", binding.Bind(models.Sender{}), register)
	m.Patch("/sender/:sender_name/validate", func(params martini.Params, r render.Render, db *mgo.Database) { validateSender(true, params, r, db) })
	m.Patch("/sender/:sender_name/invalidate", func(params martini.Params, r render.Render, db *mgo.Database) { validateSender(false, params, r, db) })
	m.Get("/sender/:sender_name", showValidate)
	m.Get("/register_consumer", register_consumer)

	m.RunOnAddr(":3000")
}

func consumeReporting() {
	fmt.Println("Started consuming")

	consumerClient, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": configuration["kafka_host"],
		"group.id":          "message_stats",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	consumerClient.SubscribeTopics([]string{configuration["reporting_queue"].(string)}, nil)

	for {
		msg, err := consumerClient.ReadMessage(-1)
		if err == nil {
			var result common_models.Message
			json.Unmarshal(msg.Value, &result)
			processDuration += result.ReceivedOn - result.CreatedOn
			processedMessages += 1

			key := strconv.FormatInt((result.ReceivedOn / 1000000000), 10)

			messagesPerSecond[key] = messagesPerSecond[key] + 1
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func health(r render.Render, db *mgo.Database) {
	healthResult := map[string]interface{}{"status": "successful"}
	if configuration != nil {
		healthResult["kafka_configuration"] = configuration
	}

	kafkaInfo, kErr := obtainKafkaServerInfo(kafkaAdminClient)
	if kErr == nil {
		healthResult["kafka_server_information"] = kafkaInfo
	}

	if processedMessages != 0 {
		healthResult["messages_latency"] = (float64(processDuration) / float64(processedMessages)) / 1000000.0
	}

	messages_sum := 0
	messages_count := 0
	for _, element := range messagesPerSecond {
		messages_sum += int(element)
		messages_count += 1
	}

	if messages_count > 0 {
		healthResult["messages_per_second"] = messages_sum / messages_count
	}

	r.JSON(200, healthResult)
}

func register_consumer(r render.Render, db *mgo.Database) {
	var err error

	var totalConsumersAmount int64 = 1
	for _, element := range registeredConsumers {
		totalConsumersAmount += int64(element)
	}

	highestDifferenceKey := ""
	previousDifference := 0
	for key, element := range queuesPriorities {
		expectedAmount := int64(math.Round(float64(totalConsumersAmount) * element))
		difference := expectedAmount - registeredConsumers[key]
		fmt.Printf("%s: %d\n", key, difference)
		if previousDifference < int(difference) {
			highestDifferenceKey = key
			previousDifference = int(difference)
		}
	}

	if highestDifferenceKey == "" {
		previousPriority := 0.0
		for key, element := range queuesPriorities {
			if element >= previousPriority {
				previousPriority = element
				highestDifferenceKey = key
			}
		}
	}

	registeredConsumers[highestDifferenceKey] += 1
	subscription_target := highestDifferenceKey

	fmt.Println(registeredConsumers)

	if err != nil {
		r.JSON(400, map[string]interface{}{"error": err.Error()})
	} else {
		r.JSON(200, map[string]interface{}{"result": "registered", "subscription_target": subscription_target})
	}
}

func reconfigure(r render.Render, db *mgo.Database) {
	success, e := setupEnvironment()
	if success {
		r.JSON(200, map[string]interface{}{"status": "successful"})
	} else {
		r.JSON(400, map[string]interface{}{"status": "failure", "error": e.Error()})
	}
}

func register(sender models.Sender, r render.Render, db *mgo.Database) {
	err := db.C("sender").Insert(sender)

	if err != nil {
		r.JSON(400, map[string]interface{}{"error": err.Error()})
	} else {
		r.JSON(200, map[string]interface{}{"result": "created new sender", "sender": sender})
	}
}

func validateSender(validateTarget bool, params martini.Params, r render.Render, db *mgo.Database) {
	var err error
	var sender models.Sender = models.Sender{}

	filter := bson.M{"name": params["sender_name"]}
	err = db.C("sender").Find(filter).One(&sender)
	if err != nil {
		r.JSON(400, map[string]interface{}{"error": err.Error()})
	} else {
		sender.Validated = validateTarget
		db.C("sender").Update(filter, sender)

		r.JSON(200, map[string]interface{}{"status": "successful", "sender": sender})
	}
}

func showValidate(params martini.Params, r render.Render, db *mgo.Database) {
	var err error
	var sender models.Sender = models.Sender{}

	filter := bson.M{"name": params["sender_name"]}
	err = db.C("sender").Find(filter).One(&sender)
	if err != nil {
		r.JSON(400, map[string]interface{}{"error": err.Error()})
	} else {
		r.JSON(200, sender)
	}
}

func setupEnvironment() (bool, error) {
	url := "http://localhost:3010/config/kafka_service_config"
	body, err := getRequest(url)

	if err != nil {
		return false, errors.New("couldn't retrieve information form provisioning service")
	}

	if body["status"] == "successful" {
		configuration = body["result"].(map[string]interface{})

		a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": configuration["kafka_host"]})
		if err != nil {
			return false, errors.New("couldn't connect to the kafka server")
		}
		kafkaAdminClient = a

		md, err := kafkaAdminClient.GetMetadata(nil, false, int(5*time.Second))
		if err != nil {
			return false, errors.New("couldn't retrieve information from the kafka server")
		}

		topics := md.Topics
		queues := configuration["queues"].([]interface{})

		createTopic(configuration["reporting_queue"].(string), 10)

		for _, queue := range queues {
			found := false
			partitionSizeDifference := 0
			info := queue.(map[string]interface{})
			for _, t := range topics {
				if t.Topic == info["name"].(string) {
					found = true
					partitionSizeDifference = len(t.Partitions) - int(info["partitions"].(float64))
					break
				}
			}

			if !found {
				createTopic(info["name"].(string), int(info["partitions"].(float64)))
			} else if partitionSizeDifference != 0 {
				removeTopic(info["name"].(string))
				createTopic(info["name"].(string), int(info["partitions"].(float64)))
			}

			registeredConsumers[info["name"].(string)] = 0
			queuesPriorities[info["name"].(string)] = info["priority"].(float64) / 100.0
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

func createTopic(name string, partitions int) ([]kafka.TopicResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := kafkaAdminClient.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: name, NumPartitions: partitions, ReplicationFactor: 1}})
	if err != nil {
		fmt.Println(err.Error())
	}

	return r, err
}

func removeTopic(name string) ([]kafka.TopicResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := kafkaAdminClient.DeleteTopics(ctx, []string{name})
	if err != nil {
		fmt.Println(err.Error())
	}

	return r, err
}

func obtainKafkaServerInfo(adminClient *kafka.AdminClient) (map[string]interface{}, error) {
	kafkaServerInformation := map[string]interface{}{}
	var kError error

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get the ClusterID
	if c, e := adminClient.ClusterID(ctx); e != nil {
		kError = e
	} else {
		kafkaServerInformation["cluster_id"] = c
	}

	// Get some metadata
	if md, e := adminClient.GetMetadata(nil, false, int(5*time.Second)); e != nil {
		kError = e
	} else {
		b := md.OriginatingBroker
		kafkaServerInformation["originating_broker_id"] = b.ID
		kafkaServerInformation["originating_broker_host"] = b.Host
		for _, b := range md.Brokers {
			kafkaServerInformation["broker_id"] = b.ID
			kafkaServerInformation["broker_host"] = b.Host
			kafkaServerInformation["broker_port"] = b.Port
		}
		kafkaServerInformation["topics"] = []interface{}{}
		for _, t := range md.Topics {
			kafkaServerInformation["topics"] = append(kafkaServerInformation["topics"].([]interface{}), map[string]interface{}{"name": t.Topic, "partition": len(t.Partitions)})
		}
	}
	return kafkaServerInformation, kError
}

func toJson(jsonString string) map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(jsonString), &result)
	return result
}
