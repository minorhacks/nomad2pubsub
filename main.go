package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/golang/glog"
)

var (
	pubsubProjectID         = flag.String("pubsub_project_id", "", "GCP project ID to use")
	pubsubTopic             = flag.String("pubsub_topic", "", "PubSub topic on which to emit messages")
	nomadAddr               = flag.String("nomad_addr", "127.0.0.1:4646", "Nomad endpoint to query for events")
	publishResultBufferSize = flag.Int("publish_result_buffer_size", 5, "Number of Publishes to do before waiting for Publishes to finish")
)

func main() {
	flag.Parse()

	if err := mainErr(context.Background()); err != nil {
		glog.Exit(err)
	}
}

type NomadEventStream struct {
	ctx context.Context
	d   json.Decoder
	r   io.ReadCloser

	eventsChan chan *NomadEvent
	errChan    chan error
}

func NewNomadEventStream(ctx context.Context, addr string, startIndex int) (*NomadEventStream, error) {
	u := &url.URL{
		Scheme: "http", // TODO: https?
		Host:   addr,
		Path:   "/v1/event/stream",
	}
	q := u.Query()
	q.Set("index", strconv.FormatInt(int64(startIndex), 10))
	u.RawQuery = q.Encode()

	res, err := http.Get(u.String())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch event stream from %q: %w", u, err)
	}

	stream := &NomadEventStream{
		ctx:        ctx,
		r:          res.Body,
		d:          *json.NewDecoder(res.Body),
		eventsChan: make(chan *NomadEvent),
		errChan:    make(chan error),
	}
	go stream.readEvents()

	return stream, nil
}

func (n *NomadEventStream) readEvents() {
	for {
		eventList := &NomadEventList{}
		err := n.d.Decode(eventList)
		if err != nil {
			n.errChan <- fmt.Errorf("failed to decode event list: %w", err)
			return
		}
		// Filter out heartbeat responses
		if len(eventList.Events) == 0 {
			continue
		}
		if glog.V(2) {
			glog.Infof("Got Events list with index %d: %d events", eventList.Index, len(eventList.Events))
		}
		for _, e := range eventList.Events {
			n.eventsChan <- e
		}
	}
}

func (n *NomadEventStream) cleanup() {
	n.r.Close()
	close(n.errChan)
	close(n.eventsChan)
}

func (n *NomadEventStream) Next() (*NomadEvent, error) {
	select {
	case err := <-n.errChan:
		if err != nil {
			glog.Errorf("NomadEventStream got error %v; cleaning up", err)
			n.cleanup()
			return nil, err
		}
		// Error was previously encountered, but Next was called anyway
		return nil, fmt.Errorf("shutdown NomadEventStream got Next() call")
	case event := <-n.eventsChan:
		return event, nil
	case <-n.ctx.Done():
		glog.Info("NomadEventStream got ctx Done; cleaning up")
		n.cleanup()
		return nil, nil
	}
}

type NomadEventList struct {
	Index  int           `json:"Index"`
	Events []*NomadEvent `json:"Events"`
}

type NomadEvent struct {
	Topic     string `json:"Topic"`
	Type      string `json:"Type"`
	Key       string `json:"Key"`
	Namespace string `json:"Namespace"`

	FilterKeys []interface{}          `json:"FilterKeys"`
	Index      int                    `json:"Index"`
	Payload    map[string]interface{} `json:"Payload"`
}

func (e *NomadEvent) Attributes() map[string]string {
	return map[string]string{
		"nomad_topic":     e.Topic,
		"nomad_type":      e.Type,
		"nomad_key":       e.Key,
		"nomad_namespace": e.Namespace,
	}
}

func (e *NomadEvent) Data() []byte {
	data, _ := json.Marshal(e)
	return data
}

type PubsubErrorHandler struct {
	results chan *pubsub.PublishResult
	ctx     context.Context
}

func NewPubsubErrorHandler(ctx context.Context, bufSize int) (*PubsubErrorHandler, error) {
	h := &PubsubErrorHandler{
		ctx:     ctx,
		results: make(chan *pubsub.PublishResult, bufSize),
	}
	go h.readResults()
	return h, nil
}

func (h *PubsubErrorHandler) readResults() {
	for {
		select {
		case <-h.ctx.Done():
			close(h.results)
			return
		case r := <-h.results:
			id, err := r.Get(h.ctx)
			if err != nil {
				glog.Errorf("PubSub publish failure: %v", err)
				// TODO: increment metric
			}
			if glog.V(2) {
				glog.Infof("PubSub publish success for ID %s", id)
			}
		}
	}
}

func (h *PubsubErrorHandler) Handle(r *pubsub.PublishResult) {
	select {
	case <-h.ctx.Done():
		return
	default:
		h.results <- r
	}
}

func mainErr(ctx context.Context) error {
	// Open pubsub topic for publishing
	client, err := pubsub.NewClient(ctx, *pubsubProjectID)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client for project %q: %w", *pubsubProjectID, err)
	}
	topic := client.Topic(*pubsubTopic)
	glog.Infof("Opened topic %q on project %q", *pubsubTopic, *pubsubProjectID)

	handler, err := NewPubsubErrorHandler(ctx, *publishResultBufferSize)
	if err != nil {
		return fmt.Errorf("failed to create pubsub error handler: %w", err)
	}

	lastSeenIndex := -1

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			stream, err := NewNomadEventStream(ctx, *nomadAddr, lastSeenIndex+1)
			if err != nil {
				return fmt.Errorf("failed to stream events from %q: %v", *nomadAddr, err)
			}

			var event *NomadEvent
			for event, err = stream.Next(); err == nil; event, err = stream.Next() {
				// Emit message on pubsub
				// TODO: Handle error
				handler.Handle(topic.Publish(ctx, &pubsub.Message{
					Attributes: event.Attributes(),
					Data:       event.Data(),
				}))
				lastSeenIndex = event.Index
			}
			glog.Errorf("Stopped reading events from stream due to error; waiting before retrying. Error: %v", err)
			time.Sleep(time.Second)
		}
	}
}
