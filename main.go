package main

import (
	"flag"
	"net/http"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Record struct {
	Name      string  `json:"name"`
	Timestamp int64   `json:"time"`
	Value     float64 `json:"value"`
	Labels    Labels  `json:"labels"`
}
type Labels map[string]string
type Records []Record

var logger *zap.Logger

func init() {
	initLogger()
}

func initLogger() {
	level := zap.NewAtomicLevel()
	level.SetLevel(zapcore.DebugLevel)

	zapConfig := zap.Config{
		Level:    level,
		Encoding: "json",
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:  "msg",
			TimeKey:     "time",
			EncodeTime:  zapcore.ISO8601TimeEncoder,
			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	l, err := zapConfig.Build()
	if err != nil {
		panic(err)
	}
	logger = l
}

func main() {
	var (
		streamName = flag.String("stream-name", "", "Kinesis stream name")
		listenAddr = flag.String("listen-addr", ":9501", "The address to listen on.")
		aws_region = flag.String("region", os.Getenv("AWS_REGION"), "AWS region name")
		//		checkpointInterval = flag.Duration("checkpoint-interval", 5*time.Second, "The interval between checkpoints.")
	)
	flag.Parse()

	if *streamName == "" {
		logger.Fatal("stream-name option is required")
	}
	config := Config{
		streamName: *streamName,
		AWSRegion:  *aws_region,
	}

	logger.Info("starting prometheus_remote_kinesis", zap.String("stream-name", *streamName))

	writer := newWriter(config)

	http.HandleFunc("/receive", writer.receive)
	http.ListenAndServe(*listenAddr, nil)
}
