package nats_messaging_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	nm "github.com/mskcc/nats-messaging-go"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestWithNats(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "nats:2.7.1",
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForLog("Server is ready"),
		Cmd: []string{
			"-js",
		},
	}

	t.Log("Given the need to test the NATs client library.")
	{
		// create the nats container
		natsC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Error(err)
		}
		defer func() {
			if err := natsC.Terminate(ctx); err != nil {
				t.Fatalf("\tfailed to terminate container: %s", err.Error())
			}
		}()

		endpoint, err := natsC.Endpoint(ctx, "")
		if err != nil {
			t.Fatalf("\tfailed to find nats server port: %s", err.Error())
		}

		m, err := nm.NewMessaging(endpoint)
		if err != nil {
			t.Errorf("\tfailed to setup NewMessaging: %s", err.Error())
		}
		defer m.Shutdown()

		m.Js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
		})

		m.Js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable: "test-con",
		})

		ch := make(chan string)
		m.Subscribe("test-con", "test.*", func(m *nm.Msg) {
			if string(m.Data) == "Test message\n" {
				ch <- "Test message received"
			} else {
				t.Errorf("\tReceived incorrect message content: %s", m.Data)
			}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
		defer cancel()

		err = m.Publish("test.dev", []byte(fmt.Sprintf("Test message\n")))
		if err == nil {
			t.Logf("\tSuccessfully published test message")
		} else {
			t.Errorf("\tFailed to publish test message: %s", err.Error())
		}

		select {
		case r := <-ch:
			t.Logf("\t%s\n", r)
		case <-ctx.Done():
			t.Errorf("\tFailed to receive the published message in allotted time")
		}
	}
}
