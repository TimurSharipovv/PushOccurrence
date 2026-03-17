package handlers

import (
	"PushOccurrence/internal/mq"
	"encoding/json"
	"os"
)

type FailedMessages struct {
	MessageId string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func WriteToFile(filename string, msg mq.Message) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(msg)
	if err != nil {
		return err
	}

	return nil
}
