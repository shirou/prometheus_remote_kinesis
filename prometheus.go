package main

import (
	"math"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func parseRecords(tss []*prompb.TimeSeries) Records {
	records := make(Records, 0, len(tss)*2)
	for _, ts := range tss {
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
			if math.IsNaN(s.Value) {
				r2.Value.Scan(nil)
			} else {
				r2.Value.Scan(s.Value)
			}
			records = append(records, r2)
		}
	}
	return records
}

var newLine = byte('\n') // for make Line-Delimited JSON
