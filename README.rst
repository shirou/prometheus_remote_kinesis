prometheus_remote_kinesis
==========================

`prometheus_remote_kinesis` is a prometheus remote storage adapter which send records to AWS Kinesis stream.

From AWS Kinesis stream, you can recieve any metrics from stream. or just store metrics to S3 via Kinesis firehose.


Usage
------

To build, use golang 1.9 or later.

::

  % go get -u
  % go build

Then just run it. If not specify port, use *9501* port.

::

   $ prometheus_remote_kinesis --stream-name prometheus-backup


You can also use Docker image.

::

   docker run -d --rm --name remote_kinesis \
      -p 9501:9501 \
      -e STREAM_NAME=prometheus-backup \
      shirou/prometheus_remote_kinesis


You can specify `prometheus.yml` like this.

::

   remote_write:
     - url: http://localhost:9501/receive



License
--------------

Apache License version 2.0
