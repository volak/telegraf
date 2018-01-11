package amon

import (
	"os"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"io/ioutil"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

type Pulse struct {
	Host		string
	StashId    	string
	Secret 		string
	
	client *http.Client
}

var sampleConfig = `
  ## Pulse Server
  host = "my-server" # required.

  ## Pulse stash
  stashid = "{guid}" # required
  
  ## Pulse secret
  secret = ""

`
type Data struct {
	Timestamp int64 `json:"timestamp"`
	Data interface{} `json:"data"`
}
type BulkEvent struct {
	StashId string `json:"stashid"`
	Secret string `json:"secret"`

	Name string `json:"name"`

	Tags map[string]string `json:"tags"`
	Data []*Data `json:"data"`
}

func (a *Pulse) Connect() error {
	if a.Host == "" || a.StashId=="" {
		return fmt.Errorf("pulse host and stashid are required fields")
	}

	a.client = &http.Client{
		Timeout:   time.Minute,
	}

	return nil
}
func (a *Pulse) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	host, e := os.Hostname()
	if e != nil {
		host = "UNKNOWN"
	}

	events := make(map[string][]*BulkEvent{})

	for _, m := range metrics {

		mname := strings.Replace(m.Name(), "_", ".", -1)
		val, ok := events[mname];
		if ok == false {
			val := &BulkEventMark{
				StashId: a.StashId,
				Secret: a.Secret,
				Name: mname,
				Tags: m.Tags(),
			}
		}

		data := &Data {
			Timestamp: m.Time().UnixNano() / (int64(time.Millisecond)/int64(time.Nanosecond)),
			Data: m.Fields(),
		}

		val.Data = append(val.Data, data)
		events[mname] = append(events[mname], val)
	}

	for k, request := range events {
		var buf bytes.Buffer
		g := gzip.NewWriter(&buf)
		if err := json.NewEncoder(g).Encode(request); err != nil {
				return fmt.Errorf("unable to encode request, %s\n", err)
		}
		if err := g.Close(); err != nil {
				return fmt.Errorf("unable to encode request, %s\n", err)
		}

		req, err := http.NewRequest("POST", fmt.Sprintf("%s/stash/%s/events?format=json", a.Host, a.StashId), &buf)
		if err != nil {
			return fmt.Errorf("unable to create http.Request, %s\n", err.Error())
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		resp, err := a.client.Do(req)
		if err == nil {
			defer resp.Body.Close()
		}

		if err != nil || resp.StatusCode != http.StatusOK {
			if err != nil {
				return fmt.Errorf("%s\n", err)
			} else if resp.StatusCode != http.StatusOK {
				
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return fmt.Errorf("Error: %s %s\n", resp.Status)
				}
				return fmt.Errorf("Error: %s %s\n", resp.Status, body)
				
			}
		}

	}
	return nil
}

func (a *Pulse) SampleConfig() string {
	return sampleConfig
}

func (a *Pulse) Description() string {
	return "Configuration for Pulse Server to send metrics to."
}


func (a *Pulse) Close() error {
	return nil
}

func init() {
	outputs.Add("pulse", func() telegraf.Output {
		return &Pulse{}
	})
}
