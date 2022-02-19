package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func init() {
	initLogger()
}

type Server struct {
	mux *http.ServeMux
}

type recordWriter interface {
	receive(w http.ResponseWriter, r *http.Request)
	close()
	run()
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

func setup(writer *recordWriter, addr string) *http.Server {
	s := &Server{
		mux: http.NewServeMux(),
	}
	s.mux.HandleFunc("/receive", (*writer).receive)
	hs := &http.Server{Addr: addr, Handler: s}
	return hs
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func main() {
	var (
		firehose      = flag.Bool("firehose", false, "Use Firehose")
		streamName    = flag.String("stream-name", "", "Kinesis stream name")
		listenAddr    = flag.String("listen-addr", ":9501", "The address to listen on.")
		aws_region    = flag.String("region", os.Getenv("AWS_REGION"), "AWS region name")
		writeInterval = flag.Duration("write-interval", 10*time.Second, "The interval between write.")
	)
	flag.Parse()

	if *streamName == "" {
		logger.Fatal("stream-name option is required")
	}
	config := Config{
		streamName:    *streamName,
		writeInterval: *writeInterval,
		AWSRegion:     *aws_region,
	}

	logger.Info("starting prometheus_remote_kinesis", zap.String("stream-name", *streamName))

	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	var writer recordWriter
	if *firehose {
		writer = newFirehoseWriter(config)
	} else {
		writer = newWriter(config)
	}

	h := setup(&writer, *listenAddr)
	go func() {
		logger.Info(fmt.Sprintf("start http server on port %s", *listenAddr))
		if err := h.ListenAndServe(); err != nil {
			logger.Warn("http server shutting down")
			if err != http.ErrServerClosed {
				logger.Fatal("closed unexpected error", zap.NamedError("error", err))
			}
			writer.close()
			h.Close()
		}
	}()

	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Shutdown(ctx)
	time.Sleep(1 * time.Second)
	logger.Warn("shutting down")
}
