# nats-messaging-go

Module nats-messaging-go implements convenience routines around the [NATS - Go Client](https://github.com/nats-io/nats.go). This package also provides support for OTel trace context propagation via NATs messages using NATS message headers.  Its intended to be used by Go apps that want to connect & communicate over NATS pub/sub without knowing much about NATS - Go Client.

## Installation

At a high level, this module depends on nats client and opentelemetry go libraries.  Running the following `go get` command will fetch these and any future dependencies as referenced in the code:

```bash
go get github.com/mskcc/nats-messaging-go
```

## Basic Usage

Code examples for connecting, publishing, and subscribing to a NATS messaging service.

```go

import (
	nm "github.com/mskcc/nats-messaging-go"
)

// without TLS
m, err := nm.NewMessaging("localhost:4222")

// with TLS
m, err := nm.NewSecureMessaging("localhost:4222", "certPath", "keyPath", "userId", "pw")
if err != nil {
	// do something
}
defer m.Shutdown()

// publish a message
err = m.Publish("subject", []byte("Hello World"))
if err != nil {
	// do something	
}

// subscribe to a subject
// consumer id much match an authorized id setup in NATS/Jetstream configuration 
m.Subscribe("consumer id", " subject", func(m *nm.Msg) {
	log.Println("Subscriber received an message via NATS on subject:", m.Subject))
	log.Println("Subscriber received an message via NATS:", string(m.Data))
})

// publish a message and propagate otel trace context
ctx, span := tracer.Start(...)
...
err = m.PublishWithTraceContext(ctx, "subject", []byte("Hello World"))
if err != nil {
    // do something
}

// subscribe to a subject and receive a propagated otel trace context
m.SubscribeWithTraceContext("consumer id", " subject", func(ctx context.Context, m *nm.Msg) {
    _, span := tracer.Start(ctx, "Nats Subscriber")_
    defer span.End()
	log.Println("Subscriber received an message via NATS on subject:", m.Subject))
	log.Println("Subscriber received a message via NATS:", string(m.Data))
    ...
})
```
