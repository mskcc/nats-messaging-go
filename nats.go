// nats_messaging implements convenience routines around the NATS - Go Client (https://github.com/nats-io/nats.go).
// provides support for OTel trace context propagation via NATs messages using NATS message headers.
// intended to be used by go apps that want to communicate via NATS pub/sub without having to know much about nats.go
package nats_messaging

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	//"time"
)

// used when pulling messages by date
const (
	pullMsgSize   = 500
	natsSubHeader = "Nats-Msg-Subject"
)

// these types are here so clients do not have to depend on nats libs when subscribing
type Msg struct {
	Subject     string
	Data        []byte
	ProviderMsg *nats.Msg
}
type MsgHandler func(msg *Msg)
type MsgHandlerWithTraceContext func(ctx context.Context, msg *Msg)

// this type represents a reference to the nats messaging client. it is obtained via
// a call to NewMessaging() and is require for all subsequent calls to pub/sub routines:
// Messaging.Publish(...), Messaging.Subscribe(...), ...
type Messaging struct {
	nc *nats.Conn
	// Js available outside package for stream & consumer management
	Js nats.JetStreamContext
}

// initializes a Messaging type. a call to this routine is a prerequisite to pub/sub operations
func NewMessaging(url string) (*Messaging, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	return &Messaging{
		nc: nc,
		Js: js,
	}, err
}

// initializes a secure/tls Messaging type. a call to this routine is a prerequisite to pub/sub operations
func NewSecureMessaging(url string, certPath, keyPath, user, pw string) (*Messaging, error) {
	cert := nats.ClientCert(certPath, keyPath)
	nc, err := nats.Connect(url, cert, nats.UserInfo(user, pw))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	return &Messaging{
		nc: nc,
		Js: js,
	}, err
}

// routine used to Publish a message (represented by []byte) on the given subject
func (m *Messaging) Publish(subj string, data []byte) error {
	msg := nats.NewMsg(subj)
	msg.Data = data
	// used when subscriber wants to filter/act on specific subject
	msg.Header.Add(natsSubHeader, subj)

	ack, err := m.Js.PublishMsg(msg)
	if err != nil {
		return err
	}
	// is this necessary or is the absence of an error good enough?
	if ack == nil {
		return errors.New("Ack never received for msg publication")
	}
	return nil
}

func (m *Messaging) PublishWithTraceContext(ctx context.Context, subj string, data []byte) error {
	propagator := otel.GetTextMapPropagator()
	headers := make(propagation.HeaderCarrier)
	propagator.Inject(ctx, headers)
	msg := &nats.Msg{
		Subject: subj,
		Data:    data,
		Header:  otelToNatsHeader(headers),
	}
	// used when subscriber wants to filter/act on a specific subject
	msg.Header.Add(natsSubHeader, subj)

	ack, err := m.Js.PublishMsg(msg)
	if err != nil {
		return err
	}
	// is this necessary or is the absence of an error good enough?
	if ack == nil {
		return errors.New("Ack never received for msg publication")
	}
	return nil
}

func (m *Messaging) Subscribe(con string, subj string, mh MsgHandler) error {

	// lets create a nats messsage handler
	// that passes the nats message content
	// to the smile message handler
	nmh := func(m *nats.Msg) {
		sm := &Msg{
			Subject:     m.Subject,
			Data:        m.Data,
			ProviderMsg: m,
		}
		mh(sm)
	}

	// subscribe to the Nats subject & register the nats message handler
	_, err := m.Js.Subscribe(subj, nmh, nats.Durable(con), nats.ManualAck())
	return err
}

func (m *Messaging) SubscribeWithTraceContext(con string, subj string, mh MsgHandlerWithTraceContext) error {

	// lets create a nats messsage handler
	// that passes the nats message content
	// to the smile message handler
	nmh := func(m *nats.Msg) {
		sm := &Msg{
			Subject:     m.Subject,
			Data:        m.Data,
			ProviderMsg: m,
		}
		propagator := otel.GetTextMapPropagator()
		headers := natsToOtelHeader(m.Header)
		ctx := context.Background()
		ctx = propagator.Extract(ctx, headers)
		mh(ctx, sm)
	}

	// subscribe to the Nats subject & register the nats message handler
	_, err := m.Js.Subscribe(subj, nmh, nats.Durable(con), nats.ManualAck())
	return err
}

// this is proof of concept.  routine needs to be cleaned up - for example, we should return Msg not nats.Msg
// func (m *Messaging) PullMsgsFromDate(t time.Time, strm string, subj string) ([]*nats.Msg, error) {
// 	s, err := m.Js.PullSubscribe(subj, "", nats.BindStream(strm), nats.StartTime(t))
// 	if err != nil {
// 		return nil, err
// 	}
// 	msgs, err := s.Fetch(pullMsgSize, nats.MaxWait(5*time.Second))
// 	if err != nil {
// 		return nil, err
// 	}
// 	for _, msg := range msgs {
// 		msg.Ack()
// 	}
// 	return msgs, nil
// }

func (m *Messaging) Shutdown() {
	m.nc.Flush()
	m.nc.Close()
	m.nc = nil
	m.Js = nil
}

// this converts otel headers to nats header for trace context propagation
func otelToNatsHeader(otelH propagation.HeaderCarrier) nats.Header {
	if otelH == nil {
		return nil
	}

	// find total number of values (each value is []string)
	nVals := 0
	for _, val := range otelH {
		nVals += len(val)
	}

	// shared slice to move header values from otel to nats
	ss := make([]string, nVals)
	// destination header
	natsH := make(nats.Header, len(otelH))

	for key, val := range otelH {
		if val == nil {
			// preserve nil values - reverse proxy distinguishes
			// between nil and zero-length header values
			natsH[key] = nil
			continue
		}

		// copy otel header val into shared slice and use as value at natsH[key]
		n := copy(ss, val)
		natsH[key] = ss[:n:n]
		// set window into shared slice to receive new otel header val
		ss = ss[n:]
	}

	return natsH
}

// this converts trace context headers delivered in a nats message to otel headers
func natsToOtelHeader(natsH nats.Header) propagation.HeaderCarrier {
	if natsH == nil {
		return nil
	}

	// find total number of values (each value is [] string)
	nVals := 0
	for _, val := range natsH {
		nVals += len(val)
	}

	// shared slice to move header values from nats to otel
	ss := make([]string, nVals)
	// destination header
	otelH := make(propagation.HeaderCarrier, len(natsH))

	for key, val := range natsH {
		if val == nil {
			// preserve nil values - reverse proxy distinguishes
			// between nil and zero-length header values
			otelH[key] = nil
			continue
		}

		// copy nats header val into shared slice and use as value at otelH[key]
		n := copy(ss, val)
		otelH[key] = ss[:n:n]
		// set window into shared slice to receive new nats header val
		ss = ss[n:]
	}

	return otelH
}
