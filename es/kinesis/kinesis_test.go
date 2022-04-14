package kinesis_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	kinesisraw "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"gitlab.b2bdev.pro/backend/go-packages/outboxer-lib/es/kinesis"
)

// Define a mock struct to be used in your unit tests of myFunc.
type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisClient) PutRecordWithContext(aws.Context, *kinesisraw.PutRecordInput, ...request.Option) (*kinesisraw.PutRecordOutput, error) {
	return &kinesisraw.PutRecordOutput{}, nil
}

func TestKinesis_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	es := kinesis.New(&mockKinesisClient{})
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			kinesis.StreamNameOption: "test",
		},
	}); err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}
}
