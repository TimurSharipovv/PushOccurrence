package config

type Config struct {
	Postgres PostgresConfig `json:"postgres"`
	RabbitMQ RabbitMQConfig `json:"rabbitmq"`
	Listener ListenerConfig `json:"listener"`
	Mongo    MongoConfig    `json:"mongo"`
}

type PostgresConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	SSLMode  string `json:"ssl_mode"`
}

type MongoConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
}

type RabbitMQConfig struct {
	Queue    QueueConfig `json:"queue"`
	Host     string      `json:"host"`
	Port     int         `json:"port"`
	User     string      `json:"user"`
	Password string      `json:"password"`
}

type QueueConfig struct {
	Name       string `json:"name"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Exclusive  bool   `json:"exclusive"`
	NoWait     bool   `json:"no_wait"`
}

type RetryConfig struct {
	BufferSize    int `json:"buffer_size"`
	RetryDelaySec int `json:"retry_delay_sec"`
	IdleSleepMs   int `json:"idle_sleep_ms"`
}

type ListenerConfig struct {
	Channels []string `json:"channels"`
}
