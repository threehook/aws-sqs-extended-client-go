package aws_sqs_extended_client_go

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/golang/mock/gomock"
	pomocks "github.com/threehook/aws-payload-offloading-go/mocks"
	sqsmocks "github.com/threehook/aws-sqs-extended-client-go/mocks"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"
)

const (
	s3BucketName         = "test-bucket-name"
	sqsSizelimit         = 262144
	moreThanSqsSizelimit = sqsSizelimit + 1
)

var (
	sqsQueueUrl = "test-queue-url"
	m           mocks
	extendedSqsWithDefaultConfig, extendedSqsWithCustomKMS,
	extendedSqsWithDefaultKMS, extendedSqsWithGenericReservedAttributeName *SqsExtendedClient
)

type mocks struct {
	ctrl             *gomock.Controller
	mockS3Client     *pomocks.MockS3SvcClientI
	mockSqsExtClient *sqsmocks.MockSqsSvcClientI
	mockS3Dao        *pomocks.MockS3DaoClientI
}

func beforeEach(t *testing.T) {
	ctrl := gomock.NewController(t)
	m = mocks{
		ctrl:             ctrl,
		mockS3Client:     pomocks.NewMockS3SvcClientI(ctrl),
		mockSqsExtClient: sqsmocks.NewMockSqsSvcClientI(ctrl),
		//mockS3Dao: pomocks.NewMockS3DaoI(ctrl),
	}
	setupClients(t)
}

func setupClients(t *testing.T) {
	//mockCtrl := gomock.NewController(t)
	//mockS3Client := pomocks.NewMockS3ClientI(mockCtrl)
	cfg, _ := NewExtendedClientConfiguration().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName)
	extendedSqsWithDefaultConfig = NewSqsExtendedClient(m.mockSqsExtClient, cfg)
}

func TestMain(m *testing.M) {
	// Suppress logging in unit tests
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}

func TestWhenSendLargeMessageThenPayloadIsStoredInS3(t *testing.T) {
	beforeEach(t)
	ctx := context.Background()

	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(1)

	messageBody := generateStringWithLength(moreThanSqsSizelimit)
	messageInput := &sqs.SendMessageInput{MessageBody: &messageBody, QueueUrl: &sqsQueueUrl}
	extendedSqsWithDefaultConfig.SendMessage(ctx, messageInput)

	input := capturedArgsMap["input"]

	if _, ok := input.(*sqs.SendMessageInput); !ok {
		t.Errorf("Expected input parameter of func 'SendMessage' to be of type '*sqs.SendMessageInput', but was of type '%s'", reflect.TypeOf(input))
	}
}

func generateStringWithLength(lngth int) string {
	runes := make([]rune, lngth, lngth)
	for i := range runes {
		runes[i] = 'x'
	}
	return string(runes)
}
