package main

import (
	"encoding/json"

	"github.com/go-martini/martini"
	"github.com/hectorandac/kafka-message-processor/provisioner/middlewares"
	"github.com/hectorandac/kafka-message-processor/provisioner/models"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/render"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// var kafkaAdminClient *kafka.AdminClient
// var kafkaDefinition common_models.KafkaDefinition

func main() {
	m := martini.Classic()
	m.Use(middlewares.MongoDB())
	m.Use(render.Renderer())

	m.Post("/config", binding.Bind(models.ConfigurationDefinition{}), ubsertConfiguration)
	m.Get("/config/:configuration_key", retrieveConfig)

	m.RunOnAddr(":3010")
}

func ubsertConfiguration(configuration models.ConfigurationDefinition, r render.Render, db *mgo.Database) {
	var err error
	var count int
	filter := bson.M{"key": configuration.Key}

	if (models.ConfigurationDefinition{}) != configuration {
		count, err = db.C("configuration_definition").Find(filter).Count()

		if err != nil {
			r.JSON(400, map[string]interface{}{"error": err.Error()})
		} else if count > 0 {
			err := db.C("configuration_definition").Update(filter, configuration)

			if err != nil {
				r.JSON(400, map[string]interface{}{"error": err.Error()})
			} else {
				r.JSON(200, map[string]interface{}{"result": "Updated existing configuration"})
			}
		} else {
			err := db.C("configuration_definition").Insert(configuration)

			if err != nil {
				r.JSON(400, map[string]interface{}{"error": err.Error()})
			} else {
				r.JSON(200, map[string]interface{}{"result": "created new configuration"})
			}
		}
	} else {
		r.JSON(400, map[string]interface{}{"error": "No difinition provided"})
	}
}

func retrieveConfig(params martini.Params, r render.Render, db *mgo.Database) {
	var err error
	var configuration models.ConfigurationDefinition = models.ConfigurationDefinition{}

	filter := bson.M{"key": params["configuration_key"]}
	err = db.C("configuration_definition").Find(filter).One(&configuration)
	if err != nil {
		r.JSON(400, map[string]interface{}{"error": err.Error()})
	} else {
		var result map[string]interface{}
		json.Unmarshal([]byte(configuration.Value), &result)

		r.JSON(200, map[string]interface{}{"status": "successful", "result": result})
	}
}
