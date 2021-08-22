package aws_sqs_extended_client_go

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/threehook/aws-payload-offloading-go/encryption"
	"github.com/threehook/aws-payload-offloading-go/payload"
	"github.com/threehook/aws-sqs-extended-client-go/appconst"
	"github.com/threehook/aws-sqs-extended-client-go/config"
	"github.com/threehook/aws-sqs-extended-client-go/mocks"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"testing"
)

const (
	s3BucketName                     = "test-bucket-name"
	sqsSizelimit                     = 262144
	moreThanSqsSizelimit             = sqsSizelimit + 1
	lessThanSqsSizeLimit             = 3
	s3ServerSideEncryption_kms_keyId = "test-customer-managed-kms-key-id"

	// should be > 1 and << SQS_SIZE_LIMIT
	arbitrarySmallerThreshold = 500
)

var (
	sqsQueueUrl = "test-queue-url"
	m           mcks
)

type mcks struct {
	ctrl             *gomock.Controller
	mockS3Client     *mocks.MockS3SvcClientI
	mockSqsExtClient *mocks.MockSqsSvcClientI
}

func beforeEach(t *testing.T) {
	ctrl := gomock.NewController(t)
	m = mcks{
		ctrl:             ctrl,
		mockS3Client:     mocks.NewMockS3SvcClientI(ctrl),
		mockSqsExtClient: mocks.NewMockSqsSvcClientI(ctrl),
	}
}

func TestMain(m *testing.M) {
	// Suppress logging in unit tests
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}

func TestWhenSendLargeMessageThenPayloadIsStoredInS3(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["sqs_input"] = input
		},
	).Times(1)

	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["s3_input"] = input
		},
	).Times(1)

	messageBody := generateStringWithLength(moreThanSqsSizelimit)
	input := &sqs.SendMessageInput{MessageBody: &messageBody, QueueUrl: &sqsQueueUrl}
	_, _ = sqsExtClient.SendMessage(ctx, input)

	sqsInput := capturedArgsMap["sqs_input"]
	if _, ok := sqsInput.(*sqs.SendMessageInput); !ok {
		t.Errorf("Expected input parameter of func 'SendMessage' to be of type '*sqs.SendMessageInput', but was of type '%s'", reflect.TypeOf(sqsInput))
	}

	s3Input := capturedArgsMap["s3_input"]
	if _, ok := s3Input.(*s3.PutObjectInput); !ok {
		t.Errorf("Expected input parameter of func 'PutObject' to be of type '*s3.PutObjectInput', but was of type '%s'", reflect.TypeOf(s3Input))
	}
}

func TestWhenSendLargeMessage_WithoutKMS_ThenPayloadIsStoredInS3AndKMSKeyIdIsNotUsed(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)

	var capturedArgsMap = make(map[string]interface{})
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["kmsKeyId"] = input.SSEKMSKeyId
		},
	).Times(1)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, input)

	assert.Nil(t, capturedArgsMap["kmsKeyId"])
}

func TestWhenSendLargeMessage_WithCustomKMS_ThenPayloadIsStoredInS3AndCorrectKMSKeyIdIsUsed(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).WithServerSideEncryptionStrategy(&encryption.CustomerKey{AwsKmsKeyId: s3ServerSideEncryption_kms_keyId}).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)

	var capturedArgsMap = make(map[string]interface{})
	//m.mockS3Client.EXPECT().PutBucketEncryption(ctx, gomock.Any())/*.Do(
	//	func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
	//		capturedArgsMap["kmsKeyId"] = input.SSEKMSKeyId
	//	},
	//).Times(1)*/
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			if input.SSEKMSKeyId != nil {
				capturedArgsMap["kmsKeyId"] = *input.SSEKMSKeyId
			}
		},
	).Times(1)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, input)

	assert.Equal(t, s3ServerSideEncryption_kms_keyId, capturedArgsMap["kmsKeyId"])
}

func TestWhenSendLargeMessage_WithDefaultKMS_ThenPayloadIsStoredInS3AndCorrectKMSKeyIdIsNotUsed(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).WithServerSideEncryptionStrategy(&encryption.AwsManagedCmk{}).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)

	var capturedArgsMap = make(map[string]interface{})
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["kmsKeyId"] = input.SSEKMSKeyId
			capturedArgsMap["bucket"] = *input.Bucket
		},
	).Times(1)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, input)

	assert.Nil(t, capturedArgsMap["kmsKeyId"])
	assert.Equal(t, s3BucketName, capturedArgsMap["bucket"])
}

func TestSendLargeMessageWithDefaultConfigThenLegacyReservedAttributeNameIsUsed(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["msgAttrs"] = input.MessageAttributes
		},
	).Times(1)

	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Times(1)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, input)

	msgAttrs := capturedArgsMap["msgAttrs"].(map[string]types.MessageAttributeValue)

	if _, ok := msgAttrs[appconst.LegacyReservedAttrName]; !ok {
		t.Errorf("Expected to find key '%s' in MessageAttributes of SendMessageInput but was not found", appconst.LegacyReservedAttrName)
	}
	if _, ok := msgAttrs[appconst.ReservedAttrName]; ok {
		t.Errorf("Expected not to find key '%s' in MessageAttributes of SendMessageInput but was found", appconst.ReservedAttrName)
	}
}

func TestSendLargeMessageWithGenericReservedAttributeNameConfigThenGenericReservedAttributeNameIsUsed(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).WithUseLegacyReservedAttributeName(false).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["msgAttrs"] = input.MessageAttributes
		},
	).Times(1)

	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Times(1)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, input)

	msgAttrs := capturedArgsMap["msgAttrs"].(map[string]types.MessageAttributeValue)

	if _, ok := msgAttrs[appconst.ReservedAttrName]; !ok {
		t.Errorf("Expected to find key '%s' in MessageAttributes of SendMessageInput but was not found", appconst.ReservedAttrName)
	}
	if _, ok := msgAttrs[appconst.LegacyReservedAttrName]; ok {
		t.Errorf("Expected not to find key '%s' in MessageAttributes of SendMessageInput but was found", appconst.LegacyReservedAttrName)
	}
}

func TestWhenSendSmallMessageThenS3IsNotUsed(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Times(0)

	msgBody := generateStringWithLength(sqsSizelimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, input)
}

func TestWhenSendMessageWithLargePayloadSupportDisabledThenS3IsNotUsedAndSqsBackendIsResponsibleToFailIt(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportDisabled().Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Times(0)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}

	_, _ = sqsExtClient.SendMessage(ctx, input)
}

func TestWhenSendMessageWithAlwaysThroughS3AndMessageIsSmallThenItIsStillStoredInS3(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		WithAlwaysThroughS3(true).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Times(1)

	msgBody := generateStringWithLength(lessThanSqsSizeLimit)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}

	_, _ = sqsExtClient.SendMessage(ctx, input)
}

func TestSendMessageWithSetMessageSizeThresholdThenThresholdIsHonored(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		WithPayloadSizeThreshold(arbitrarySmallerThreshold).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)

	var capturedArgsMap = make(map[string]interface{})
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["s3_input"] = input
		},
	).Times(1)

	msgBody := generateStringWithLength(arbitrarySmallerThreshold * 2)
	input := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}

	_, _ = sqsExtClient.SendMessage(ctx, input)

	s3Input := capturedArgsMap["s3_input"]
	if _, ok := s3Input.(*s3.PutObjectInput); !ok {
		t.Errorf("Expected input parameter of func 'PutObject' to be of type '*s3.PutObjectInput', but was of type '%s'", reflect.TypeOf(s3Input))
	}
}

func TestReceiveMessageMultipleTimesDoesNotAdditionallyAlterReceiveMessageInput(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	output := &sqs.ReceiveMessageOutput{}
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().ReceiveMessage(ctx, gomock.Any()).Return(output, nil).Do(
		func(ctx context.Context, input *sqs.ReceiveMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(2)

	input := &sqs.ReceiveMessageInput{}
	expectedInput := &sqs.ReceiveMessageInput{MessageAttributeNames: appconst.ReservedAttrNames}

	_, _ = sqsExtClient.ReceiveMessage(ctx, input)
	assert.Equal(t, expectedInput, capturedArgsMap["input"])
	_, _ = sqsExtClient.ReceiveMessage(ctx, input)
	assert.Equal(t, expectedInput, capturedArgsMap["input"])
}

func TestReceiveMessage_when_MessageIsLarge_legacyReservedAttributeUsed(t *testing.T) {
	beforeEach(t)
	_ = testReceiveMessage_when_MessageIsLarge(t, appconst.LegacyReservedAttrName)
}

func TestReceiveMessage_when_MessageIsLarge_ReservedAttributeUsed(t *testing.T) {
	beforeEach(t)
	_ = testReceiveMessage_when_MessageIsLarge(t, appconst.ReservedAttrName)
}

func testReceiveMessage_when_MessageIsLarge(t *testing.T, reservedAttributeName string) error {
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	msgAttrValues := make(map[string]types.MessageAttributeValue)
	msgAttrValues[reservedAttributeName] = types.MessageAttributeValue{}
	receiptHandle := uuid.New().String()
	msg := &types.Message{MessageAttributes: msgAttrValues, ReceiptHandle: &receiptHandle}
	pointer := payload.PayloadS3Pointer{S3BucketName: s3BucketName, S3Key: "S3key"}
	json, err := pointer.ToJson()
	if err != nil {
		return err
	}
	msg.Body = &json

	ctx := context.Background()
	output := &sqs.ReceiveMessageOutput{Messages: []types.Message{*msg}}
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().ReceiveMessage(ctx, gomock.Any()).Return(output, nil).Do(
		func(ctx context.Context, input *sqs.ReceiveMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(1)

	expectedMsg := "LargeMessage"
	s3Object := &s3.GetObjectOutput{Body: ioutil.NopCloser(bytes.NewReader([]byte(expectedMsg)))}
	m.mockS3Client.EXPECT().GetObject(ctx, gomock.Any()).Return(s3Object, nil).Do(
		func(ctx context.Context, input *s3.GetObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(1)

	input := &sqs.ReceiveMessageInput{}
	o, err := sqsExtClient.ReceiveMessage(ctx, input)
	actualMsg := o.Messages[0]

	assert.Equal(t, *msg.Body, *actualMsg.Body)

	msgAttrsContainsKey := false
	for _, k := range appconst.ReservedAttrNames {
		if _, ok := actualMsg.MessageAttributes[k]; ok {
			msgAttrsContainsKey = true
		}
	}
	assert.False(t, msgAttrsContainsKey)

	s3Input := capturedArgsMap["input"]
	if _, ok := s3Input.(*s3.GetObjectInput); !ok {
		t.Errorf("Expected input parameter of func 'GetObject' to be of type '*s3.GetObjectInput', but was of type '%s'", reflect.TypeOf(s3Input))
	}

	return nil
}

func TestReceiveMessage_when_MessageIsSmall(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	expectedMsg := "SmallMessage"
	msgAttrValues := make(map[string]types.MessageAttributeValue)
	expectedMsgAttrName := "AnyMessageAttribute"
	msgAttrValues[expectedMsgAttrName] = types.MessageAttributeValue{}
	output := &sqs.ReceiveMessageOutput{Messages: []types.Message{types.Message{Body: &expectedMsg, MessageAttributes: msgAttrValues}}}
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().ReceiveMessage(ctx, gomock.Any()).Return(output, nil).Do(
		func(ctx context.Context, input *sqs.ReceiveMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(1)

	m.mockS3Client.EXPECT().GetObject(ctx, gomock.Any()).Times(0)

	input := &sqs.ReceiveMessageInput{}
	o, _ := sqsExtClient.ReceiveMessage(ctx, input)
	actualMsg := o.Messages[0]

	assert.Equal(t, expectedMsg, *actualMsg.Body)
	_, expMsgAttrValue := actualMsg.MessageAttributes[expectedMsgAttrName]
	assert.NotNil(t, expMsgAttrValue)

	msgAttrsContainsKey := false
	for _, k := range appconst.ReservedAttrNames {
		if _, ok := actualMsg.MessageAttributes[k]; ok {
			msgAttrsContainsKey = true
		}
	}
	assert.False(t, msgAttrsContainsKey)

	sqsInput := capturedArgsMap["input"]
	if _, ok := sqsInput.(*sqs.ReceiveMessageInput); !ok {
		t.Errorf("Expected input parameter of func 'ReceiveMessage' to be of type '*sqs.ReceiveMessageInput', but was of type '%s'", reflect.TypeOf(sqsInput))
	}
}

func TestWhenMessageBatchIsSentThenOnlyMessagesLargerThanThresholdAreStoredInS3(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	// This creates 10 messages, out of which only two are below the threshold (100K and 200K),
	// and the other 8 are above the threshold
	msgLengthForCounter := []int{
		100_000,
		300_000,
		400_000,
		500_000,
		600_000,
		700_000,
		800_000,
		900_000,
		200_000,
		1000_000,
	}
	batchEntries := make([]types.SendMessageBatchRequestEntry, 10)
	for i := 0; i < 10; i++ {
		msgBody := generateStringWithLength(msgLengthForCounter[i])
		entryId := fmt.Sprintf("entry_%d", i)
		entry := types.SendMessageBatchRequestEntry{MessageBody: &msgBody, Id: &entryId}
		batchEntries[i] = entry
	}

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessageBatch(ctx, gomock.Any()).Times(1)
	var capturedArgsMap = make(map[string]interface{})
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(8)

	params := &sqs.SendMessageBatchInput{QueueUrl: &sqsQueueUrl, Entries: batchEntries}
	_, _ = sqsExtClient.SendMessageBatch(ctx, params)

	s3Input := capturedArgsMap["input"]
	if _, ok := s3Input.(*s3.PutObjectInput); !ok {
		t.Errorf("Expected input parameter of func 'PutObject' to be of type '*s3.PutObjectInput', but was of type '%s'", reflect.TypeOf(s3Input))
	}
}

func TestWhenSmallMessageIsSentThenNoAttributeIsAdded(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["msgAttrs"] = input.MessageAttributes
		},
	).Times(1)

	msgBody := generateStringWithLength(lessThanSqsSizeLimit)
	params := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, params)

	msgAttrs := capturedArgsMap["msgAttrs"]
	assert.Empty(t, msgAttrs)
}

func TestWhenLargeMessageIsSentThenAttributeWithPayloadSizeIsAdded(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["msgAttrs"] = input.MessageAttributes
		},
	).Times(1)

	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Times(1)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	params := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, params)

	msgAttrs := capturedArgsMap["msgAttrs"].(map[string]types.MessageAttributeValue)
	actualMsglength, _ := strconv.Atoi(*msgAttrs[appconst.LegacyReservedAttrName].StringValue)
	assert.Equal(t, moreThanSqsSizelimit, actualMsglength)
}

func TestDefaultExtendedClientDeletesSmallMessage(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().DeleteMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.DeleteMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["rcptHndl"] = input.ReceiptHandle
		},
	).Times(1)
	m.mockS3Client.EXPECT().DeleteObject(ctx, gomock.Any()).Times(0)

	receiptHandle := uuid.New().String()
	params := &sqs.DeleteMessageInput{QueueUrl: &sqsQueueUrl, ReceiptHandle: &receiptHandle}

	_, _ = sqsExtClient.DeleteMessage(ctx, params)

	actualReceiptHandle := capturedArgsMap["rcptHndl"].(*string)
	assert.Equal(t, receiptHandle, *actualReceiptHandle)
}

func TestDefaultExtendedClientDeletesObjectS3UponMessageDelete(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().DeleteMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.DeleteMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["rcptHndl"] = input.ReceiptHandle
		},
	).Times(1)
	m.mockS3Client.EXPECT().DeleteObject(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *s3.DeleteObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(1)

	randomS3Key := uuid.New().String()
	originalReceiptHandle := uuid.New().String()
	largeMsgReceiptHandle := getLargeReceiptHandle(randomS3Key, originalReceiptHandle)

	params := &sqs.DeleteMessageInput{QueueUrl: &sqsQueueUrl, ReceiptHandle: &largeMsgReceiptHandle}
	_, _ = sqsExtClient.DeleteMessage(ctx, params)

	actualReceiptHandle := capturedArgsMap["rcptHndl"].(*string)
	assert.Equal(t, originalReceiptHandle, *actualReceiptHandle)
	s3Input := capturedArgsMap["input"].(*s3.DeleteObjectInput)
	assert.Equal(t, s3BucketName, *s3Input.Bucket)
	assert.Equal(t, randomS3Key, *s3Input.Key)
}

func TestExtendedClientConfiguredDoesNotDeleteObjectFromS3UponDelete(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		WithCleanupS3Payload(false).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	ctx := context.Background()
	var capturedArgsMap = make(map[string]interface{})
	m.mockSqsExtClient.EXPECT().DeleteMessage(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *sqs.DeleteMessageInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["rcptHndl"] = input.ReceiptHandle
		},
	).Times(1)
	m.mockS3Client.EXPECT().DeleteObject(ctx, gomock.Any()).Times(0)

	randomS3Key := uuid.New().String()
	originalReceiptHandle := uuid.New().String()
	largeMsgReceiptHandle := getLargeReceiptHandle(randomS3Key, originalReceiptHandle)

	params := &sqs.DeleteMessageInput{QueueUrl: &sqsQueueUrl, ReceiptHandle: &largeMsgReceiptHandle}
	_, _ = sqsExtClient.DeleteMessage(ctx, params)

	actualReceiptHandle := capturedArgsMap["rcptHndl"].(*string)
	assert.Equal(t, originalReceiptHandle, *actualReceiptHandle)
}

func TestDefaultExtendedClientDeletesObjectsFromS3UponDeleteBatch(t *testing.T) {
	beforeEach(t)
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).Build()
	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	batchSize := 10
	params := generateLargeDeleteBatchRequest(batchSize)

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().DeleteMessageBatch(ctx, gomock.Any()).Times(1)
	var capturedArgsMap = make(map[string]interface{})
	m.mockS3Client.EXPECT().DeleteObject(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *s3.DeleteObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["bucket"] = input.Bucket
		},
	).Times(batchSize)

	_, _ = sqsExtClient.DeleteMessageBatch(ctx, params)

	bucket := capturedArgsMap["bucket"].(*string)
	assert.Equal(t, s3BucketName, *bucket)
}

func TestWhenSendMessageWIthCannedAccessControlListDefined(t *testing.T) {
	beforeEach(t)
	expectedObjectCannedACL := s3Types.ObjectCannedACLBucketOwnerFullControl
	cfg, _ := config.NewExtendedClientConfigBuilder().WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		WithCannedAccessControlList(expectedObjectCannedACL).Build()

	ctx := context.Background()
	m.mockSqsExtClient.EXPECT().SendMessage(ctx, gomock.Any()).Times(1)
	var capturedArgsMap = make(map[string]interface{})
	m.mockS3Client.EXPECT().PutObject(ctx, gomock.Any()).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["input"] = input
		},
	).Times(1)

	sqsExtClient := NewSqsExtendedClient(m.mockSqsExtClient, cfg)

	msgBody := generateStringWithLength(moreThanSqsSizelimit)
	params := &sqs.SendMessageInput{QueueUrl: &sqsQueueUrl, MessageBody: &msgBody}
	_, _ = sqsExtClient.SendMessage(ctx, params)

	s3Input := capturedArgsMap["input"].(*s3.PutObjectInput)
	assert.Equal(t, expectedObjectCannedACL, s3Input.ACL)
}

func generateStringWithLength(lngth int) string {
	runes := make([]rune, lngth, lngth)
	for i := range runes {
		runes[i] = 'x'
	}
	return string(runes)
}

func getLargeReceiptHandle(s3Key, originalReceiptHandle string) string {
	return appconst.S3BucketNameMarker + s3BucketName + appconst.S3BucketNameMarker + appconst.S3KeyMarker + s3Key +
		appconst.S3KeyMarker + originalReceiptHandle
}

func generateLargeDeleteBatchRequest(size int) *sqs.DeleteMessageBatchInput {
	deleteEntries := make([]types.DeleteMessageBatchRequestEntry, size)
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("%d", i)
		rcptHndl := getSampleLargeReceiptHandle()
		entry := &types.DeleteMessageBatchRequestEntry{
			Id:            &id,
			ReceiptHandle: &rcptHndl,
		}
		deleteEntries[i] = *entry
	}
	return &sqs.DeleteMessageBatchInput{Entries: deleteEntries, QueueUrl: &sqsQueueUrl}
}

func getSampleLargeReceiptHandle() string {
	return getLargeReceiptHandle(uuid.New().String(), uuid.New().String())
}
