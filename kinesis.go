package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"go.uber.org/zap"
)

const MaxNumberOfBuffer = 1000

type Config struct {
	streamName string
	AWSRegion  string
}

type kinesisWriter struct {
	svc        *kinesis.Kinesis
	streamName string
	writeCh    chan Records
}

func newWriter(config Config) *kinesisWriter {
	awsConfig := aws.NewConfig()
	if config.AWSRegion != "" {
		awsConfig = awsConfig.WithRegion(config.AWSRegion)
	} else {
		awsConfig = awsConfig.WithRegion("ap-northeast-1")
	}

	svc := session.Must(session.NewSessionWithOptions(session.Options{
		Config: *awsConfig,
	}))

	w := kinesisWriter{
		streamName: config.streamName,
		svc:        kinesis.New(svc),
		writeCh:    make(chan Records, MaxNumberOfBuffer),
	}

	go w.run()

	return &w
}

func (w *kinesisWriter) run() {
	for {
		select {
		case records, ok := <-w.writeCh:
			if !ok {
				return
			}
			if err := w.write(records, w.svc); err != nil {
				logger.Error("write failed", zap.NamedError("error", err))
			}
		}
	}
}

var newLine = byte('\n') // for make Line-Delimited JSON

func (w *kinesisWriter) write(records Records, svc *kinesis.Kinesis) error {
	rs := make([]*kinesis.PutRecordsRequestEntry, len(records))
	for i, record := range records {
		// json
		j, err := json.Marshal(record)
		if err != nil {
			logger.Error("marshal error", zap.NamedError("error", err))
			continue
		}
		fmt.Println(string(j))

		// compress to gzip
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(append(j, newLine))
		w.Close()

		rs[i] = &kinesis.PutRecordsRequestEntry{
			Data:         b.Bytes(),
			PartitionKey: aws.String(record.Name),
		}
	}
	input := &kinesis.PutRecordsInput{
		Records:    rs,
		StreamName: aws.String(w.streamName),
	}

	_, err := w.svc.PutRecords(input)
	return err
}
