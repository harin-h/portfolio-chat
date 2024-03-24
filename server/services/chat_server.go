package services

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/harin-h/logs"
	_ "github.com/harin-h/utils"
	"github.com/segmentio/kafka-go"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type chatServer struct {
}

func NewChatServer() ChatServer {
	return chatServer{}
}

var incrementId uint64

type OwnerReplyMessageUnit struct {
	ChatId         uint64
	Message        string
	EventTimestamp time.Time
}

type OwnerReplyMessageHandle struct {
	MessageQue []OwnerReplyMessageUnit
	mu         sync.Mutex
}

type ClientReplyMessageUnit struct {
	ClientId       uint64
	Message        string
	EventTimestamp time.Time
}

type ClientReplyMessageHandle struct {
	MessageQue []ClientReplyMessageUnit
	mu         sync.Mutex
}

var ctx context.Context
var redisConn *redis.Client
var kafkaConn *kafka.Conn
var ownerReplyMessageHandle OwnerReplyMessageHandle
var clientReplyMessageHandle ClientReplyMessageHandle
var clientOnlineList map[int]bool

func Initiate(redisConn_ *redis.Client, kafkaConn_ *kafka.Conn) {
	logs.Info("Initiated Server...")
	ctx = context.Background()
	redisConn = redisConn_
	kafkaConn = kafkaConn_
	clientOnlineList = make(map[int]bool)
}

func GetCountVisitor() int {
	return int(incrementId)
}

// Visitor Chat
func (chatServer) ClientChat(stream Chat_ClientChatServer) error {

	// visitor id
	clientId := incrementId
	incrementId += 1
	logs.Info(fmt.Sprint("Client Id -", clientId, ": Starting..."))

	// for checking online list
	clientOnlineList[int(clientId)] = true

	// for closing the chat function
	ctxCancal, cancel := context.WithCancel(ctx)

	// line message notice (kafka-request)
	if kafkaConn == nil {
		logs.Error("No Notice Line Message Cause of Failed Connection")
	} else {
		messages := []kafka.Message{
			{
				Value: []byte("New Chat : timestamp " + time.Now().Format("2006-01-02 15:04:05")),
			},
		}
		kafkaConn.WriteMessages(messages...)
	}

	// finish step : change status in online list and add system message in chat
	defer func() {
		clientOnlineList[int(clientId)] = false
		clientReplyMessageHandle.mu.Lock()
		messageUnit := ClientReplyMessageUnit{
			ClientId:       clientId,
			Message:        "[System Log] Client Closed",
			EventTimestamp: time.Now(),
		}
		clientReplyMessageHandle.MessageQue = append(clientReplyMessageHandle.MessageQue, messageUnit)
		clientReplyMessageHandle.mu.Unlock()
		logs.Info(fmt.Sprint("Client Id -", clientId, ": Closed"))
	}()

	done := make(chan bool, 1)
	errs := make(chan error, 1)

	// visitor sender message : add messages to message list and record in redis
	go func() {
		for {
			logs.Info(fmt.Sprint("Client Id -", clientId, ": Request Starting"))
			req, err := stream.Recv()
			if err != nil {
				cancel()
				logs.Error(fmt.Sprint("Client Id -", clientId, ": Request Error - Log Error", err))
				errs <- err
			}
			if req == nil {
				cancel()
				logs.Error(fmt.Sprint("Client Id -", clientId, ": Client Closing"))
				done <- true
				break
			}
			logs.Info(fmt.Sprint("Client Id -", clientId, ": Request Body - ", *req))
			clientReplyMessageHandle.mu.Lock()
			messageUnit := ClientReplyMessageUnit{
				ClientId:       clientId,
				Message:        req.Message,
				EventTimestamp: req.EventTimestamp.AsTime().Local(),
			}
			clientReplyMessageHandle.MessageQue = append(clientReplyMessageHandle.MessageQue, messageUnit)
			clientReplyMessageHandle.mu.Unlock()
			logs.Info(fmt.Sprint("Client Id -", clientId, ": Added Message Que"))
			logs.Info(fmt.Sprint("Client Id -", clientId, ": Redis Updating"))
			cachedData, err := redisConn.Get(ctx, "client").Result()
			messageUnitArray := []ClientReplyMessageUnit{}
			if err == redis.Nil {
				messageUnitArray = append(messageUnitArray, messageUnit)
				messageUnitArrayByte, err := json.Marshal(messageUnitArray)
				if err != nil {
					cancel()
					logs.Error(fmt.Sprint("Client Id -", clientId, ": JSON Marsshal Failed - Log Error", err))
					errs <- err
				}
				err = redisConn.Set(ctx, "client", string(messageUnitArrayByte), 0).Err()
				if err != nil {
					cancel()
					logs.Error(fmt.Sprint("Client Id -", clientId, ": Redis Failed Set - Log Error", err))
					errs <- err
				}
			} else if err != nil {
				cancel()
				logs.Error(fmt.Sprint("Client Id -", clientId, ": Redis Failed Get - Log Error", err))
				errs <- err
			} else {
				err := json.Unmarshal([]byte(cachedData), &messageUnitArray)
				if err != nil {
					cancel()
					logs.Error(fmt.Sprint("Client Id -", clientId, ": JSON Unmarsshal Failed - Log Error", err))
					errs <- err
				}
				messageUnitArray = append(messageUnitArray, messageUnit)
				messageUnitArrayByte, err := json.Marshal(messageUnitArray)
				if err != nil {
					cancel()
					logs.Error(fmt.Sprint("Client Id -", clientId, ": JSON Marsshal Failed - Log Error", err))
					errs <- err
				}
				err = redisConn.Set(ctx, "client", string(messageUnitArrayByte), 0).Err()
				if err != nil {
					cancel()
					logs.Error(fmt.Sprint("Client Id -", clientId, ": Redis Failed Set - Log Error", err))
					errs <- err
				}
			}
			logs.Info(fmt.Sprint("Client Id -", clientId, ": Redis Updated"))
			logs.Info(fmt.Sprint("Client Id -", clientId, ": Request Successed"))
		}
	}()

	// visitor receive message : get owner reply from message list and send to visitor
	go func() {
		for {
			select {
			case <-ctxCancal.Done():
				return
			default:
				for {
					logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Starting"))
					time.Sleep(500 * time.Millisecond)
					ownerReplyMessageHandle.mu.Lock()
					if len(ownerReplyMessageHandle.MessageQue) == 0 {
						ownerReplyMessageHandle.mu.Unlock()
						logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Closed - No Reply"))
						break
					}
					chatId := ownerReplyMessageHandle.MessageQue[0].ChatId
					message := ownerReplyMessageHandle.MessageQue[0].Message
					eventTimestamp := ownerReplyMessageHandle.MessageQue[0].EventTimestamp
					isOnline, prs := clientOnlineList[int(chatId)]
					if prs && !isOnline {
						if len(ownerReplyMessageHandle.MessageQue) > 1 {
							ownerReplyMessageHandle.MessageQue = ownerReplyMessageHandle.MessageQue[1:]
						} else {
							ownerReplyMessageHandle.MessageQue = []OwnerReplyMessageUnit{}
						}
						logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Closed - Client", chatId, "Closed"))
						ownerReplyMessageHandle.mu.Unlock()
						continue
					}
					if chatId == clientId {
						logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Getting - New Reply"))
						res := ClientChatResponse{
							Message:        message,
							EventTimestamp: timestamppb.New(eventTimestamp),
						}
						err := stream.Send(&res)
						if err != nil {
							logs.Error(fmt.Sprint("Client Id -", clientId, ": Receive Failed- Log Error", err))
							errs <- err
						}
						logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Received -", res))
						logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Deleting Message Que"))
						if len(ownerReplyMessageHandle.MessageQue) > 1 {
							ownerReplyMessageHandle.MessageQue = ownerReplyMessageHandle.MessageQue[1:]
						} else {
							ownerReplyMessageHandle.MessageQue = []OwnerReplyMessageUnit{}
						}
						logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Deleted Message Que"))
					} else {
						logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Passed - Not for this Client"))
					}
					ownerReplyMessageHandle.mu.Unlock()
					logs.Info(fmt.Sprint("Client Id -", clientId, ": Receive Successed"))
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	select {
	case <-done:
		return nil
	case err := <-errs:
		return err
	}
}

// Owner Chat
func (chatServer) OwnerChat(stream Chat_OwnerChatServer) error {

	// for closing the chat function
	logs.Info("Owner : Starting...")
	ctxCancal, cancel := context.WithCancel(ctx)

	done := make(chan bool)
	errs := make(chan error)

	// owner sender message : add messages to message list and record in redis
	go func() {
		for {
			logs.Info("Owner : Request Starting")
			req, err := stream.Recv()
			if err != nil {
				logs.Error(fmt.Sprint("Owner : Request Error - Log Error", err))
				errs <- err
			}
			if req == nil {
				cancel()
				logs.Error("Owner : Owner Closing")
				done <- true
				break
			}
			logs.Info(fmt.Sprint("Owner : Request Body - ", *req))
			ownerReplyMessageHandle.mu.Lock()
			messageUnit := OwnerReplyMessageUnit{
				ChatId:         req.ChatId,
				Message:        req.Message,
				EventTimestamp: req.EventTimestamp.AsTime().Local(),
			}
			ownerReplyMessageHandle.MessageQue = append(ownerReplyMessageHandle.MessageQue, messageUnit)
			ownerReplyMessageHandle.mu.Unlock()
			logs.Info("Owner : Added Message Que")
			logs.Info("Owner : Redis Updating")
			cachedData, err := redisConn.Get(ctx, "owner").Result()
			messageUnitArray := []OwnerReplyMessageUnit{}
			if err == redis.Nil {
				messageUnitArray = append(messageUnitArray, messageUnit)
				messageUnitArrayByte, err := json.Marshal(messageUnitArray)
				if err != nil {
					logs.Error(fmt.Sprint("Owner: JSON Marsshal Failed - Log Error", err))
					errs <- err
				}
				err = redisConn.Set(ctx, "owner", string(messageUnitArrayByte), 0).Err()
				if err != nil {
					logs.Error(fmt.Sprint("Owner : Redis Failed Set - Log Error", err))
					errs <- err
				}
			} else if err != nil {
				logs.Error(fmt.Sprint("Owner : Redis Failed Get - Log Error", err))
				errs <- err
			} else {
				err := json.Unmarshal([]byte(cachedData), &messageUnitArray)
				if err != nil {
					logs.Error(fmt.Sprint("Owner: JSON Unmarsshal Failed - Log Error", err))
					errs <- err
				}
				messageUnitArray = append(messageUnitArray, messageUnit)
				messageUnitArrayByte, err := json.Marshal(messageUnitArray)
				if err != nil {
					logs.Error(fmt.Sprint("Owner: JSON Marsshal Failed - Log Error", err))
					errs <- err
				}
				err = redisConn.Set(ctx, "owner", string(messageUnitArrayByte), 0).Err()
				if err != nil {
					logs.Error(fmt.Sprint("Owner : Redis Failed Set - Log Error", err))
					errs <- err
				}
			}
			logs.Info("Owner : Redis Updated")
			logs.Info("Owner : Request Successed")
		}
	}()

	// owner receive message : get visitor reply from message list and send to owner
	go func() {
		logs.Info("Owner : Receive Initiating...")
		clientReplyMessageHandle.mu.Lock()
		time.Sleep(500 * time.Millisecond)
		logs.Info("Owner : Receive Getting Redis Starter")
		cachedData, err := redisConn.Get(ctx, "client").Result()
		if err != nil && err != redis.Nil {
			clientReplyMessageHandle.mu.Unlock()
			logs.Error(fmt.Sprint("Owner : Receive Getting Redis Starter Failed - Log Error", err))
			errs <- err
		} else if err == nil {
			err := json.Unmarshal([]byte(cachedData), &clientReplyMessageHandle.MessageQue)
			if err != nil {
				cancel()
				clientReplyMessageHandle.mu.Unlock()
				logs.Error(fmt.Sprint("Owner : JSON Marsshal Failed - Log Error", err))
				errs <- err
			}
		}
		clientReplyMessageHandle.mu.Unlock()
		for {
			select {
			case <-ctxCancal.Done():
				return
			default:
				for {
					logs.Info("Owner : Receive Starting")
					time.Sleep(500 * time.Millisecond)
					clientReplyMessageHandle.mu.Lock()
					if len(clientReplyMessageHandle.MessageQue) == 0 {
						logs.Info("Owner : Receive Closed - No Reply")
						clientReplyMessageHandle.mu.Unlock()
						break
					}
					logs.Info("Owner : Receive Getting - New Reply")
					clientId := clientReplyMessageHandle.MessageQue[0].ClientId
					message := clientReplyMessageHandle.MessageQue[0].Message
					eventTimestamp := clientReplyMessageHandle.MessageQue[0].EventTimestamp
					res := OwnerChatResponse{
						ChatId:         clientId,
						Message:        message,
						EventTimestamp: timestamppb.New(eventTimestamp),
					}
					err := stream.Send(&res)
					if err != nil {
						logs.Error(fmt.Sprint("Owner : Receive Failed- Log Error", err))
						errs <- err
					}
					logs.Info(fmt.Sprint("Owner : Receive Received -", res))
					logs.Info("Owner : Receive Deleting Message Que")
					if len(clientReplyMessageHandle.MessageQue) > 1 {
						clientReplyMessageHandle.MessageQue = clientReplyMessageHandle.MessageQue[1:]
					} else {
						clientReplyMessageHandle.MessageQue = []ClientReplyMessageUnit{}
					}
					logs.Info("Owner : Receive Deleted Message Que")
					clientReplyMessageHandle.mu.Unlock()
					logs.Info("Owner : Receive Successed")
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	select {
	case <-done:
		return nil
	case err := <-errs:
		return err
	}
}

func (chatServer) mustEmbedUnimplementedChatServer() {}
