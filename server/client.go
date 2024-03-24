package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"server/services"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	url := flag.String("URL", "localhost:50051", "Service Url")
	flag.Parse()
	cc, err := grpc.Dial(*url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer cc.Close()

	client := services.NewChatClient(cc)
	streamClient, err := client.ClientChat(context.Background())
	if err != nil {
		panic(err)
	}
	x := ClientService{stream: streamClient}
	go x.SendMessage()
	go x.ReceiveMessage()
	defer x.stream.CloseSend()
	bl := make(chan bool)
	<-bl
}

type ClientService struct {
	stream services.Chat_ClientChatClient
}

func (ch *ClientService) SendMessage() {
	for {
		reader := bufio.NewReader(os.Stdin)
		clientMessage, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf(" Failed to read from console :: %v", err)
		}
		clientMessage = strings.Trim(clientMessage, "\r\n")

		clientMessageBox := &services.ClientChatRequest{
			Message:        clientMessage,
			EventTimestamp: timestamppb.New(time.Now()),
		}

		err = ch.stream.Send(clientMessageBox)

		if err != nil {
			log.Printf("Error while sending message to server :: %v", err)
		}

	}
}

func (ch *ClientService) ReceiveMessage() {

	//create a loop
	for {
		mssg, err := ch.stream.Recv()
		if err != nil {
			log.Printf("Error in receiving message from server :: %v", err)
		}

		//print message to console
		fmt.Printf("Message %s - Time %s \n", mssg.Message, mssg.EventTimestamp.AsTime().String())

	}
}
