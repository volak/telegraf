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
type Datapoint struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Timestamp int64 `json:"timestamp"`
	Value interface{} `json:"value"`
}
type Mark struct {
	StashId string `json:"stashid"`
	Secret string `json:"secret"`
	Source string `json:"source"`
	Timestamp int64 `json:"timestamp"`

	Tags map[string]string `json:"tags"`
	Datapoints []*Datapoint `json:"datapoints"`
}
type Marks struct {
	StashId string `json:"stashid"`
	Marks []*Mark `json:"marks"`
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

	marks := []*Mark{}
	for _, m := range metrics {
		mark := &Mark{
			StashId: a.StashId,
			Secret: a.Secret,
			Source: host,
			Timestamp: m.Time().UnixNano() / (int64(time.Millisecond)/int64(time.Nanosecond)),
			Tags: m.Tags(),
		}

		mname := strings.Replace(m.Name(), "_", ".", -1)
		if dps, err := buildMetrics(mname, m, m.Time()); err == nil {
			mark.Datapoints = dps
		} else {
			log.Printf("unable to build Metric for %s, skipping\n", m.Name())
		}

		marks = append(marks, mark)
	}

	request := Marks{
		StashId: a.StashId,
		Marks: marks,
	}
	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)
	if err := json.NewEncoder(g).Encode(request); err != nil {
			return fmt.Errorf("unable to encode request, %s\n", err)
	}
	if err := g.Close(); err != nil {
			return fmt.Errorf("unable to encode request, %s\n", err)
	}

		req, err := http.NewRequest("POST", fmt.Sprintf("%s/stash/%s/marks?format=json", a.Host, a.StashId), &buf)
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

	return nil
}

func (a *Pulse) SampleConfig() string {
	return sampleConfig
}

func (a *Pulse) Description() string {
	return "Configuration for Pulse Server to send metrics to."
}


func buildMetrics(name string, m telegraf.Metric, now time.Time) ([]*Datapoint, error) {
	dps := []*Datapoint{}
	for k, v := range m.Fields() {
		dp := &Datapoint {
			Name: name + "_" + k,
			Timestamp: now.UnixNano() / (int64(time.Millisecond)/int64(time.Nanosecond)),
			Value: v,
		}
		dps = append(dps, dp)
	}
	return dps, nil
}


func (a *Pulse) Close() error {
	return nil
}

func init() {
	outputs.Add("pulse", func() telegraf.Output {
		return &Pulse{}
	})
}
