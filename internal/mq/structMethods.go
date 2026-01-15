package mq

/* func (m *Mq) Publish(payload []byte) error {
	err := m.Channel.Publish(
		"",
		m.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
		},
	)
	if err != nil {
		log.Printf("Publish error, pushing to retryBuffer: %v", err)

		m.SetConnected(false)

		select {
		case m.retryBuffer <- payload:
		default:
			log.Println("retryBuffer full, dropping message")
		}

		return err
	}

	return nil
}

func (m *Mq) Close() {
	if m.Channel != nil {
		m.Channel.Close()
	}
	if m.Conn != nil {
		m.Conn.Close()
	}
}

// Тестовый набор
func (m *Mq) RetryBuffer() <-chan []byte {
	return m.retryBuffer
} */
