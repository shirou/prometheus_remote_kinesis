package main

import (
	"database/sql"
	"encoding/json"
)

type Record struct {
	Name      string          `json:"name"`
	Timestamp int64           `json:"time"`
	Value     JsonNullFloat64 `json:"value"`
	Labels    Labels          `json:"labels"`
}
type Labels map[string]string
type Records []Record

type JsonNullFloat64 struct {
	sql.NullFloat64
}

func (v JsonNullFloat64) MarshalJSON() ([]byte, error) {
	if v.Valid {
		return json.Marshal(v.Float64)
	} else {
		return json.Marshal(nil)
	}
}

func (v *JsonNullFloat64) UnmarshalJSON(data []byte) error {
	var x *float64
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	if x != nil {
		v.Valid = true
		v.Float64 = *x
	} else {
		v.Valid = false
	}
	return nil
}
