package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"math"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

const MaxNumberOfBuffer = 1000
const DefaultAWSRegion = "ap-northeast-1"

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
		awsConfig = awsConfig.WithRegion(DefaultAWSRegion)
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		cred := credentials.NewEnvCredentials()
		awsConfig = awsConfig.WithCredentials(cred)
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

func (writer *kinesisWriter) receive(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Error("read failed", zap.NamedError("error", err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		logger.Error("decode failed", zap.NamedError("error", err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		logger.Error("unmarshal failed", zap.NamedError("error", err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	records := make(Records, 0, len(req.Timeseries))
	for _, ts := range req.Timeseries {
		var r Record
		m := make(Labels, len(ts.Labels))
		for _, l := range ts.Labels {
			m[string(model.LabelName(l.Name))] = string(model.LabelValue(l.Value))
		}
		r.Labels = m
		r.Name = m["__name__"]
		if len(ts.Samples) == 0 {
			records = append(records, r)
			continue
		}
		// flatten for each ts.Samples
		for _, s := range ts.Samples {
			r2 := r
			r2.Timestamp = s.Timestamp
			if !math.IsNaN(s.Value) {
				r2.Value = s.Value
			}
			records = append(records, r2)
		}
	}

	writer.writeCh <- records
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
