package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/smtp"
	"os"
	"time"

	"github.com/harin-h/logs"
	"github.com/jordan-wright/email"
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
		Topic:     "Email",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
		Dialer:    dialer,
	})

	// send email (kafka-receive)
	logs.Info("Starting...")
	for {
		m, err := r.ReadMessage(context.Background())
		logs.Info("New Receive...")
		if err != nil {
			logs.Info("Failed Kafka Read - Log Error : " + err.Error())
			break
		}
		logs.Info("Receive Body : " + string(m.Value))
		e := email.NewEmail()
		e.From = fmt.Sprintf("%s <%s>", "BOBBY BOT", "good.harisombut@gmail.com")
		e.Subject = "Summary Visitor Chat of Portfolio (" + time.Now().Format("2006-01-02") + ")"
		e.HTML = []byte(`Summary Visitor Chat of ` + time.Now().Format("2006-01-02"))
		e.To = []string{"good.harisombut@gmail.com"}
		_, err = e.AttachFile("../src/" + string(m.Value))
		if err != nil {
			logs.Error("Attach File Failed : " + err.Error())
			continue
		}
		smtpAuth := smtp.PlainAuth("", "good.harisombut@gmail.com", "vkqubqawqxiajsso", "smtp.gmail.com")
		err = e.Send("smtp.gmail.com:587", smtpAuth)
		if err != nil {
			logs.Error("Sending Failed : " + err.Error())
			continue
		}
		logs.Info("Sending Success")
	}

	if err := r.Close(); err != nil {
		logs.Error("failed to close reader:" + err.Error())
		panic(err)
	}
	logs.Info("Closed...")
}
