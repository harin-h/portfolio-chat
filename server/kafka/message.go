package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/harin-h/logs"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func main() {

	// environment variable

	// get kafka url
	kafkaUrl := os.Getenv("KAFKA")
	if kafkaUrl == "" {
		panic(fmt.Errorf("kafka's url is not found"))
	}

	// get username for connecting kafka
	username := os.Getenv("USERNAME")
	if username == "" {
		panic(fmt.Errorf("kafka's username is not found"))
	}

	// get password for connecting kafka
	password := os.Getenv("PASSWORD")
	if password == "" {
		panic(fmt.Errorf("kafka's password is not found"))
	}

	// kafka connect
	mechanism := plain.Mechanism{
		Username: username,
		Password: password,
	}
	dialer := &kafka.Dialer{
		DualStack:     true,
		SASLMechanism: mechanism,
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaUrl},
		Topic:     "LineMessage",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
		Dialer:    dialer,
	})

	// line message notice (kafka-receive)
	logs.Info("Starting...")
	for {
		m, err := r.ReadMessage(context.Background())
		logs.Info("New Receive...")
		if err != nil {
			logs.Info("Failed Kafka Read - Log Error : " + err.Error())
			break
		}
		logs.Info("Receive Body : " + string(m.Value))
		type LineMessage struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		type LinePush struct {
			To       string        `json:"to"`
			Messages []LineMessage `json:"messages"`
		}
		message := LineMessage{Type: "text", Text: string(m.Value)}
		linePush := LinePush{To: "Ub16a9c4b87071fb17cbfd8b472be5485", Messages: []LineMessage{message}}
		reqBody, _ := json.Marshal(linePush)
		client := &http.Client{}
		logs.Info("Request Body : " + string(reqBody))
		req, err := http.NewRequest("POST", "https://api.line.me/v2/bot/message/push", bytes.NewBuffer(reqBody))
		if err != nil {
			logs.Error("Failed Creating Requester - Log Error : " + err.Error())
			continue
		}
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Authorization", "Bearer +fSP/Kd9/iRijbaR67TnfEhIVSquPwdudTMDaRXNiI4IjBxJt3PNLl5tOjTnUCWF8PYF+Rh7bJtt6HIFxTSXp7nUeVHvOiKcfYU9pVwtajFFjyRBcwQip735UjCtufnAiid+Hj7LUA4I/I4YNC5sXwdB04t89/1O/w1cDnyilFU=")

		_, err = client.Do(req)
		if err != nil {
			logs.Error("Failed Line API - Log Error : " + err.Error())
		}
		logs.Info("Line API Success")
	}

	if err := r.Close(); err != nil {
		logs.Error("failed to close reader:" + err.Error())
		panic(err)
	}
	logs.Info("Closed...")
}
