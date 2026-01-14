package mq

import (
	"context"
	"log"
	"time"
)

func (m *Mq) MonitorConnection(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("monitoring stopped")
			return

		case <-ticker.C:
			m.mu.Lock()
			conn := m.Conn
			ch := m.Channel
			m.mu.Unlock()

			if conn == nil || ch == nil {
				m.SetConnected(false)
				continue
			}

			if conn.IsClosed() || ch.IsClosed() {
				log.Println("conn lost detected by monitor")
				m.SetConnected(false)
			}
		}
	}
}
