package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	proto "github.com/golang/protobuf/proto"
	uuid "github.com/google/uuid"
	model "github.com/rnzsgh/apig-kds-load-generator/protob/model"
)

const maxPayloadInBytes = 25000

const messagesToSend = 100000000

const producerCount = 100

var streamName string // KDS

var apiUrl string

/*

  curl -H "Content-Type: application/x-amz-json-1.1" -X POST \
    -d "{ \"PartitionKey\": \"$(uuidgen)\", \"StreamName\": \"$STREAM_NAME\", \"Data\": \"$(echo '{ "yea": 1, "nay": 0, "eventId": "newnewnewtest", "userId": "somethingnew" }' | base64)\" }" $URL

*/

type KdsMessage struct {
	PartitionKey string `json:"PartitionKey"`
	StreamName   string `json:"StreamName"`
	Data         string `json:"Data"`
}

func main() {

	flag.StringVar(&streamName, "s", "test", "Specify the stream. Default is 'test'")
	flag.StringVar(&apiUrl, "u", "NOT_SET", "Specify the url. Default is 'NOT_SET'")
	flag.Parse()

	if apiUrl == "NOT_SET" {
		log.Fatalln("Api URL not set")
	}

	rand.Seed(time.Now().UnixNano())
	channel := make(chan int)

	for i := 0; i < producerCount; i++ {
		go generateLoad(channel)
	}

	for i := 0; i < messagesToSend; i++ {
		channel <- randomSize()
	}

	close(channel)
}

func generateLoad(channel chan int) {

	var events []*model.Event
	var fill int

	messageOverheadBytes := 200 // This is a guess - figure out message overhead

	for size := range channel {

		body := randomBody(size)

		if fill+size+messageOverheadBytes > maxPayloadInBytes {
			if err := prepareAndSend(events); err != nil {
				log.Fatalln(err)
			}
			events = nil
			fill = 0
		}

		fill += size + fill + messageOverheadBytes

		events = append(events, &model.Event{
			Id:  uuid.NewString(),
			Raw: body,
		})
	}

	// Send the last message
	if len(events) > 0 {

		if err := prepareAndSend(events); err != nil {
			log.Fatalln(err)
		}

	}
}

func prepareAndSend(events []*model.Event) error {

	out, err := prepareMessage(events)
	if err != nil {
		return err
	}

	_, err = http.Post(apiUrl, "application/json", bytes.NewReader(out))
	if err != nil {
		return err
	}

	//fmt.Println("sent")
	return nil
}

func prepareMessage(events []*model.Event) ([]byte, error) {

	group := &model.Group{
		Events: events,
	}
	out, err := proto.Marshal(group)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal event: %v", err)
	}

	message := &KdsMessage{
		PartitionKey: uuid.NewString(),
		StreamName:   streamName,
		Data:         base64.StdEncoding.EncodeToString(out),
	}

	res, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal message: %v", err)
	}

	return res, nil
}

func randomSize() int {
	length := rand.Int31n(25000)

	if length == 0 {
		length = 200
	}

	return int(length)
}

func randomBody(size int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, size)

	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}

	return string(s)
}
