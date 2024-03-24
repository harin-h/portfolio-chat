package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"portfolio-chat/services"
	"strconv"
	"time"

	"github.com/harin-h/logs"
	"github.com/harin-h/utils"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/xuri/excelize/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {

	// environment variable

	// get port of server
	port := flag.String("PORT", ":50051", "Port")

	// get redis url
	redisUrl := os.Getenv("REDIS")
	if redisUrl == "" {
		panic(fmt.Errorf("redis's url is not found"))
	}

	// get database url
	databaseUrl := os.Getenv("DATABASE")
	if databaseUrl == "" {
		panic(fmt.Errorf("database's url is not found"))
	}

	// get kafka url
	kafkaUrl := os.Getenv("KAFKA")
	if kafkaUrl == "" {
		panic(fmt.Errorf("kafka's url is not found"))
	}

	// get secret key for authorization
	secretKey := os.Getenv("SECRET_KEY")
	if secretKey == "" {
		panic(fmt.Errorf("secret key is not found"))
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

	// redis connect
	redisConn := RedisConnect(redisUrl)

	// kafka connect topic:LineMessage
	kafkaLineMessageConn := KafkaConnect(kafkaUrl, username, password, "LineMessage")

	// finish step after server down
	defer func() {

		// line message notice (kafka-request)
		NoticeFinish(kafkaLineMessageConn)

		// kafka connect topic:Email
		kafkaEmailConn := KafkaConnect(kafkaUrl, username, password, "Email")

		// record all chat from redis to database and then attach all chat and send email (kafka-request)
		RecordDatabaseAndEEmailFinish(redisConn, databaseUrl, kafkaEmailConn)

	}()

	//  send all connection to services
	services.Initiate(redisConn, kafkaLineMessageConn)

	// create server
	s := grpc.NewServer(
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if info.FullMethod == "/services.Chat/OwnerChat" {
				md, prs := metadata.FromIncomingContext(ss.Context())
				if !prs {
					return status.Errorf(codes.Unauthenticated, "metadata is not provided")
				}
				values, prs := md["cookie"]
				if !prs {
					return status.Errorf(codes.Unauthenticated, "authorization token is not provided")
				}
				tokenString := values[0]
				isValid, err := utils.ValidateToken(tokenString, secretKey)
				if !isValid || err != nil {
					return status.Errorf(codes.Unauthenticated, "authorization token is invalid")
				}
			}
			return handler(srv, ss)
		}),
	)

	// create port for serving
	listener, err := net.Listen("tcp", *port)
	if err != nil {
		logs.Error("Failed to create a listener")
		panic(err)
	}

	// registry chat services to server
	services.RegisterChatServer(s, services.NewChatServer())

	// controller server down
	go func() {
		time.Sleep(time.Hour)
		s.Stop()
	}()

	// start serving server
	logs.Info("Portfolio-Chat (gRPC) : Serving... in port " + *port)
	err = s.Serve(listener)
	if err != nil {
		logs.Error("Failed to serve a server")
		panic(err)
	}

}

// redis connect
func RedisConnect(redisUrl string) *redis.Client {
	logs.Info("Redis Connecting...")
	opt, err := redis.ParseURL(redisUrl)
	if err != nil {
		panic("Finish Step : Inacceptable Redis Url - Log Error : " + err.Error())
	}
	redisConn := redis.NewClient(opt)
	return redisConn
}

// kafka connect
func KafkaConnect(kafkaUrl string, username string, password string, topic string) *kafka.Conn {
	logs.Info("Kafka Topic : " + topic + " - Connecting...")
	dialer := &kafka.Dialer{
		DualStack: true,
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
		SASLMechanism: plain.Mechanism{
			Username: username,
			Password: password,
		},
	}
	conn, err := dialer.DialLeader(context.Background(), "tcp", kafkaUrl, topic, 0)
	if err != nil {
		logs.Error("kafka connection failed - Log Error : " + err.Error())
		return nil
	}
	return conn
}

// line message notice (kafka-request)
func NoticeFinish(conn *kafka.Conn) {
	if conn == nil {
		logs.Error("Finish Step : No Notice Line Message Cause of Failed Connection")
		return
	}
	logs.Info("Finish Step : Hand to Noticing Line Message Starting...")
	messages := []kafka.Message{}
	x := recover()
	_, ok := x.(error)
	if ok {
		messages = []kafka.Message{
			{
				Value: []byte("API Broke!!!"),
			},
		}
	} else {
		messages = []kafka.Message{
			{
				Value: []byte("Number of Visitors:" + strconv.Itoa(services.GetCountVisitor())),
			},
		}
	}
	conn.WriteMessages(messages...)
	logs.Info("Finish Step : Hand to Noticing Line Message Finished")
}

// record all chat from redis to database and then attach all chat and send email (kafka-request)
func RecordDatabaseAndEEmailFinish(redisConn *redis.Client, databaseUrl string, kafkaConn *kafka.Conn) {

	type message struct {
		gorm.Model
		ChatId         int
		Message        string
		EventTimestamp time.Time
		IsOwner        int
	}
	messages := []message{}
	logs.Info("Finish Step : Redis Connecting...")

	// get all client reply messages of this open server
	clientMessageUnitArray := []services.ClientReplyMessageUnit{}
	clientCachedData, err := redisConn.Get(context.Background(), "client").Result()
	if err != nil && err != redis.Nil {
		logs.Error(fmt.Sprint("Finish Step : Can not get data from Redis - Log Error", err))
		return
	} else if err != redis.Nil {
		err := json.Unmarshal([]byte(clientCachedData), &clientMessageUnitArray)
		if err != nil {
			logs.Error(fmt.Sprint("Finish Step : JSON Unmarsshal Failed - Log Error", err))
			return
		}
	}

	// get all owner reply messages of this open server
	ownerMessageUnitArray := []services.OwnerReplyMessageUnit{}
	ownerCachedData, err := redisConn.Get(context.Background(), "owner").Result()
	if err != nil && err != redis.Nil {
		logs.Error(fmt.Sprint("Finish Step : Can not get data from Redis - Log Error", err))
		return
	} else if err != redis.Nil {
		err := json.Unmarshal([]byte(ownerCachedData), &ownerMessageUnitArray)
		if err != nil {
			logs.Error(fmt.Sprint("Finish Step : JSON Unmarsshal Failed - Log Error", err))
			return
		}
	}

	// data preparation
	for _, v := range clientMessageUnitArray {
		messages = append(messages, message{ChatId: int(v.ClientId), Message: v.Message, EventTimestamp: v.EventTimestamp, IsOwner: 0})
	}
	for _, v := range ownerMessageUnitArray {
		messages = append(messages, message{ChatId: int(v.ChatId), Message: v.Message, EventTimestamp: v.EventTimestamp, IsOwner: 1})
	}

	// database connect
	logs.Info("Finish Step : Database Connecting...")
	db, err := gorm.Open(postgres.Open(databaseUrl), &gorm.Config{})
	if err != nil {
		logs.Error(fmt.Sprint("Finish Step : Database Connecting Failed - Log Error", err))
		return
	}

	// record all messages on database
	db.AutoMigrate(&message{})
	tx := db.Begin()
	for _, v := range messages {
		if result := tx.Create(&v); result.Error != nil {
			tx.Rollback()
			logs.Error(fmt.Sprint("Finish Step : Failed Recording Database - Log Error", err))
			return
		}
	}
	if err := tx.Commit().Error; err != nil {
		logs.Error(fmt.Sprint("Finish Step : Failed Recording Database - Log Error", err))
		return
	}
	logs.Info("Finish Step : Recording Database Success")

	// get all sorted messages from database
	logs.Info("Finish Step : Getting Sorted Data from Database...")
	db.Where("DATE(created_at) = DATE(CURRENT_TIMESTAMP)").Order("chat_id, event_timestamp").Find(&messages)

	// generate a all chat report as Excel file
	logs.Info("Finish Step : Creating Summary Excel...")
	f := excelize.NewFile()
	index, err := f.NewSheet("Sheet1")
	if err != nil {
		logs.Error(fmt.Sprint("Finish Step : Failed Creating Excel - Log Error", err))
		return
	}
	f.SetActiveSheet(index)
	f.SetCellValue("Sheet1", "A1", "Chat Id")
	f.SetCellValue("Sheet1", "B1", "Is Owner")
	f.SetCellValue("Sheet1", "C1", "Message")
	f.SetCellValue("Sheet1", "D1", "Event Timestamp")
	f.SetColWidth("Sheet1", "C", "C", 30)
	f.SetColWidth("Sheet1", "D", "D", 15)
	i := 2
	for _, v := range messages {
		f.SetCellValue("Sheet1", "A"+strconv.Itoa(i), v.ChatId)
		f.SetCellValue("Sheet1", "B"+strconv.Itoa(i), v.IsOwner)
		f.SetCellValue("Sheet1", "C"+strconv.Itoa(i), v.Message)
		f.SetCellValue("Sheet1", "D"+strconv.Itoa(i), v.EventTimestamp)
		i += 1
	}
	fileName := time.Now().Format("2006-01-02") + "_summary.xlsx"
	if err := f.SaveAs("src/" + fileName); err != nil {
		logs.Error(fmt.Sprint("Finish Step : Failed Creating Excel - Log Error", err))
		return
	}
	logs.Info("Finish Step : Creating Summary Excel Success")

	// send email (kafka-request)
	logs.Info("Finish Step : Hand to Sending Email Report Starting...")
	kafkaMessages := []kafka.Message{
		{
			Value: []byte(fileName),
		},
	}
	if kafkaConn == nil {
		logs.Error("Finish Step : No Send Email Report Cause of Failed Connection")
		return
	}
	kafkaConn.WriteMessages(kafkaMessages...)
	logs.Info("Finish Step : Hand to Sending Email Report Finished")
}
