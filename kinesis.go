package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
 	"sync"
	"time"
        "fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
	"strconv"
	"math/rand"
)

const MaxNumberOfBuffer = 1000
const MaxPutRecordsSize = 4500000 // 5MB
const MaxPutRecordsEntries = 500
const DefaultAWSRegion = "ap-northeast-1"

type Config struct {
	streamName    string
	writeInterval time.Duration
	AWSRegion     string
}

type kinesisWriter struct {
	svc           *kinesis.Kinesis
	writeInterval time.Duration
	streamName    string
	writeCh       chan Records
	mutex         sync.Mutex
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
		streamName:    config.streamName,
		svc:           kinesis.New(svc),
		writeInterval: config.writeInterval,
		writeCh:       make(chan Records, MaxNumberOfBuffer),
	}
	fmt.Printf("record: %+v\n", w)                                                                                                                                                                           
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

	records := parseRecords(req.Timeseries)

	writer.writeCh <- records
}

func (w *kinesisWriter) close() {
	close(w.writeCh)
}

func (w *kinesisWriter) run() {
	rs := make([]*kinesis.PutRecordsRequestEntry, 0, MaxPutRecordsEntries)
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
			rs = make([]*kinesis.PutRecordsRequestEntry, 0, MaxPutRecordsEntries)
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
			if bytes+l > MaxPutRecordsSize ||
				len(rs)+len(tmp) > MaxPutRecordsEntries {
				logger.Debug("send",
					zap.Int("bytes", bytes),
					zap.Int("entries", len(rs)),
				)
				if err := w.send(rs, w.svc); err != nil {
					logger.Error("send failed", zap.NamedError("error", err))
				}
				bytes = 0
				rs = make([]*kinesis.PutRecordsRequestEntry, 0, MaxPutRecordsEntries)
			}
			bytes += l
			rs = append(rs, tmp...)

			w.mutex.Unlock()
		}
	}
}

func (w *kinesisWriter) write(records Records) ([]*kinesis.PutRecordsRequestEntry, int) {
	rs := make([]*kinesis.PutRecordsRequestEntry, len(records))
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
		partition := strconv.Itoa(rand.Intn(256))
	        logger.Info(fmt.Sprintf("PartitionKey: %+v\n", partition))	
		rs[i] = &kinesis.PutRecordsRequestEntry{
			Data:         j,
			PartitionKey: aws.String(partition),
		}
		//		l += b.Len()
		l += len(j)
	}
	return rs, l
}

func (w *kinesisWriter) send(entries []*kinesis.PutRecordsRequestEntry, svc *kinesis.Kinesis) error {
	if len(entries) == 0 {
		return nil
	}
	input := &kinesis.PutRecordsInput{
		Records:    entries,
		StreamName: aws.String(w.streamName),
	}


	//_, err := w.svc.PutRecords(input)
	resp, err := w.svc.PutRecords(input)
	logger.Info(fmt.Sprintf("kinesis response: %+v\n", resp))
	//fmt.Printf("resp: %+v\n", resp)
	return err
}
