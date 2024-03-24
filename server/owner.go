package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"server/services"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	url := flag.String("URL", "localhost:50051", "Service Url")
	token := flag.String("AUTH", "*", "Authorization")
	flag.Parse()
	cc, err := grpc.Dial(*url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer cc.Close()

	client := services.NewChatClient(cc)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "Cookie", *token)
	streamOwner, err := client.OwnerChat(ctx)
	if err != nil {
		panic(err)
	}
	x := OwnerService{stream: streamOwner}
	go x.SendMessage()
	go x.ReceiveMessage()
	bl := make(chan bool)
	<-bl
}

type OwnerService struct {
	stream services.Chat_OwnerChatClient
}

func (ch *OwnerService) SendMessage() {
	for {
		readerId := bufio.NewReader(os.Stdin)
		chatIdString, err := readerId.ReadString('\n')
		chatIdString = strings.TrimSpace(chatIdString)
		if err != nil {
			log.Fatalf(" Failed to read from console :: %v", err)
		}
		clientId, _ := strconv.Atoi(chatIdString)
		readerMsg := bufio.NewReader(os.Stdin)
		ownerMessage, err := readerMsg.ReadString('\n')
		if err != nil {
			log.Fatalf(" Failed to read from console :: %v", err)
		}
		ownerMessage = strings.Trim(ownerMessage, "\r\n")
		ownerMessageBox := &services.OwnerChatRequest{
			ChatId:         uint64(clientId),
			Message:        ownerMessage,
			EventTimestamp: timestamppb.New(time.Now()),
		}

		err = ch.stream.Send(ownerMessageBox)

		if err != nil {
			log.Printf("Error while sending message to server :: %v", err)
		}

	}
}

func (ch *OwnerService) ReceiveMessage() {

	//create a loop
	for {
		mssg, err := ch.stream.Recv()
		if err != nil {
			log.Printf("Error in receiving message from server :: %v", err)
		}

		//print message to console
		fmt.Printf("Chat Id %s - Message %s - Time %s\n", strconv.Itoa(int(mssg.ChatId)), mssg.Message, mssg.EventTimestamp.AsTime().String())

	}
}
