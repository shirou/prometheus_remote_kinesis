package main

import (
	"flag"
	"io/ioutil"
	"math"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/prometheus/prometheus/prompb"
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

func main() {
	var (
		listenAddr = flag.String("listen-addr", ":1234", "The address to listen on.")
		streamName = flag.String("stream-name", "", "Kinesis stream name")
		//		checkpointInterval = flag.Duration("checkpoint-interval", 5*time.Second, "The interval between checkpoints.")
	)
	flag.Parse()

	if *streamName == "" {
		logger.Fatal("stream-name option is required")
	}
	config := Config{
		streamName: *streamName,
	}

	writer := newWriter(config)

	http.HandleFunc("/receive", writer.receive)
	http.ListenAndServe(*listenAddr, nil)
}
