package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

const MaxNumberOfFirehoseBuffer = 1000
const MaxPutRecordBatchSize = 4500000 // 5MB
const MaxPutRecordBatchEntries = 500

type firehoseWriter struct {
	svc           *firehose.Firehose
	writeInterval time.Duration
	streamName    string
	writeCh       chan Records
	mutex         sync.Mutex
}

func newFirehoseWriter(config Config) *firehoseWriter {
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

	w := firehoseWriter{
		streamName:    config.streamName,
		svc:           firehose.New(svc),
		writeInterval: config.writeInterval,
		writeCh:       make(chan Records, MaxNumberOfFirehoseBuffer),
	}

	go w.run()

	return &w
}

func (writer *firehoseWriter) receive(w http.ResponseWriter, r *http.Request) {
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

	records := parseRecords(req.Timeseries)

	writer.writeCh <- records
}

func (w *firehoseWriter) close() {
	close(w.writeCh)
}

func (w *firehoseWriter) run() {
	rs := make([]*firehose.Record, 0, MaxPutRecordBatchEntries)
	var bytes int

	ticker := time.NewTicker(w.writeInterval)

	for {
		select {
		case <-ticker.C:
			w.mutex.Lock()
			if err := w.send(rs, w.svc); err != nil {
				logger.Error("send failed", zap.NamedError("error", err))
			}
			bytes = 0
			rs = make([]*firehose.Record, 0, MaxPutRecordBatchEntries)
			w.mutex.Unlock()
		case records, ok := <-w.writeCh:
			if !ok {
				logger.Warn("write channel closed. send current buffer")
				if err := w.send(rs, w.svc); err != nil {
					logger.Error("send failed", zap.NamedError("error", err))
				}
				return
			}

			tmp, l := w.write(records)

			w.mutex.Lock()
			if len(rs) == 0 {
				bytes += l
				rs = append(rs, tmp...)
				w.mutex.Unlock()
				continue
			}
			if bytes+l > MaxPutRecordBatchSize ||
				len(rs)+len(tmp) > MaxPutRecordBatchEntries {
				logger.Debug("send",
					zap.Int("bytes", bytes),
					zap.Int("entries", len(rs)),
				)
				if err := w.send(rs, w.svc); err != nil {
					logger.Error("send failed", zap.NamedError("error", err))
				}
				bytes = 0
				rs = make([]*firehose.Record, 0, MaxPutRecordBatchEntries)
			}
			bytes += l
			rs = append(rs, tmp...)

			w.mutex.Unlock()
		}
	}
}

func (w *firehoseWriter) write(records Records) ([]*firehose.Record, int) {
	rs := make([]*firehose.Record, len(records))
	var l int
	for i, record := range records {
		// to json
		j, err := json.Marshal(record)
		if err != nil {
			logger.Error("marshal error", zap.NamedError("error", err))
			continue
		}
		j = append(j, newLine)
		// compress to gzip
		/*
			var b bytes.Buffer
			w := gzip.NewWriter(&b)
			w.Write(append(j, newLine))
			w.Close()
		*/

		rs[i] = &firehose.Record{
			Data: j,
		}
		//		l += b.Len()
		l += len(j)
	}
	return rs, l
}

func (w *firehoseWriter) send(entries []*firehose.Record, svc *firehose.Firehose) error {
	if len(entries) == 0 {
		return nil
	}
	input := &firehose.PutRecordBatchInput{
		Records:            entries,
		DeliveryStreamName: aws.String(w.streamName),
	}

	_, err := w.svc.PutRecordBatch(input)
	return err
}
