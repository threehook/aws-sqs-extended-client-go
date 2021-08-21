package aws_sqs_extended_client_go

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/threehook/aws-payload-offloading-go/payload"
	"github.com/threehook/aws-payload-offloading-go/s3"
	"github.com/threehook/aws-sqs-extended-client-go/appconst"
	"github.com/threehook/aws-sqs-extended-client-go/util"
	"log"
	"strconv"
	"strings"
)

type SqsExtendedClient struct {
	//private static final Log LOG = LogFactory.getLog(AmazonSQSExtendedClient.class);
	SqsClient       SqsSvcClientI
	SqsClientConfig *ExtendedClientConfig
	PayloadStore    payload.PayloadStore
}

// Constructs a new Amazon SQS extended client to invoke service methods on Amazon SQS with extended functionality
// using the specified Amazon SQS client object.
func NewSqsExtendedClient(sqsClient SqsSvcClientI, config *ExtendedClientConfig) *SqsExtendedClient {
	s3Dao := &s3.S3Dao{
		S3Client:                     config.S3Client,
		ServerSideEncryptionStrategy: config.ServerSideEncryptionStrategy,
		ObjectCannedACL:              config.ObjectCannedACL,
	}

	return &SqsExtendedClient{
		SqsClient:       sqsClient,
		SqsClientConfig: config,
		PayloadStore: &payload.S3BackedPayloadStore{
			S3BucketName: config.S3BucketName,
			S3Dao:        s3Dao,
		},
	}
}

func (c *SqsExtendedClient) AddPermission(ctx context.Context, params *sqs.AddPermissionInput, optFns ...func(*sqs.Options)) (*sqs.AddPermissionOutput, error) {
	return c.SqsClient.AddPermission(ctx, params, optFns...)
}

func (c *SqsExtendedClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return c.SqsClient.ChangeMessageVisibility(ctx, params, optFns...)
}

func (c *SqsExtendedClient) ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	return c.SqsClient.ChangeMessageVisibilityBatch(ctx, params, optFns...)
}

func (c *SqsExtendedClient) CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	return c.SqsClient.CreateQueue(ctx, params, optFns...)
}

// Deletes the specified message from the specified queue and deletes the message payload from Amazon S3 when necessary.
// You specify the message by using the message's receipt handle and not the message ID you received when
// you sent the message. Even if the message is locked by another reader due to the visibility timeout setting,
// it is still deleted from the queue. If you leave a message in the queue for longer than the queue's configured
// retention period, Amazon SQS automatically deletes it.
func (c *SqsExtendedClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if params == nil {
		err := errors.New("DeleteMessageInput cannot be nil")
		log.Println(err)
		return nil, err
	}

	middleware.AddUserAgentKeyValue("USER_AGENT", appconst.UserAgentHdr)

	if !c.SqsClientConfig.PayloadSupport {
		return c.SqsClient.DeleteMessage(ctx, params, optFns...)
	}

	receiptHandle := *params.ReceiptHandle
	origReceiptHandle := receiptHandle

	// Update original receipt handle if needed
	if c.isS3ReceiptHandle(receiptHandle) {
		origReceiptHandle = c.getOrigReceiptHandle(receiptHandle)
		// Delete payload from S3 if needed
		if c.SqsClientConfig.CleanupS3Payload {
			msgPointer, err := c.getMessagePointerFromModifiedReceiptHandle(receiptHandle)
			if err != nil {
				return nil, err
			}
			c.PayloadStore.DeleteOriginalPayload(msgPointer)
		}
	}
	params.ReceiptHandle = &origReceiptHandle
	return c.SqsClient.DeleteMessage(ctx, params, optFns...)
}

// Deletes up to ten messages from the specified queue. This is a batch version of DeleteMessage.
// The result of the delete action on each message is reported individually in the response.
// Also deletes the message payloads from Amazon S3 when necessary.
func (c *SqsExtendedClient) DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	if params == nil {
		err := errors.New("DeleteMessageBatchInput cannot be nil")
		log.Println(err)
		return nil, err
	}

	middleware.AddUserAgentKeyValue("USER_AGENT", appconst.UserAgentHdr)

	if !c.SqsClientConfig.PayloadSupport {
		return c.SqsClient.DeleteMessageBatch(ctx, params, optFns...)
	}

	for _, entry := range params.Entries {
		receiptHandle := *entry.ReceiptHandle
		origReceiptHandle := receiptHandle

		// Update original receipt handle if needed
		if c.isS3ReceiptHandle(receiptHandle) {
			origReceiptHandle = c.getOrigReceiptHandle(receiptHandle)
			// Delete payload from S3 if needed
			if c.SqsClientConfig.CleanupS3Payload {
				msgPointer, err := c.getMessagePointerFromModifiedReceiptHandle(receiptHandle)
				if err != nil {
					return nil, err
				}
				c.PayloadStore.DeleteOriginalPayload(msgPointer)
			}
		}
		entry.ReceiptHandle = &origReceiptHandle
	}

	return c.SqsClient.DeleteMessageBatch(ctx, params, optFns...)
}

func (c *SqsExtendedClient) DeleteQueue(ctx context.Context, params *sqs.DeleteQueueInput, optFns ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error) {
	return c.SqsClient.DeleteQueue(ctx, params, optFns...)
}

func (c *SqsExtendedClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	return c.SqsClient.GetQueueAttributes(ctx, params, optFns...)
}

func (c *SqsExtendedClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return c.SqsClient.GetQueueUrl(ctx, params, optFns...)
}

func (c *SqsExtendedClient) ListDeadLetterSourceQueues(ctx context.Context, params *sqs.ListDeadLetterSourceQueuesInput, optFns ...func(*sqs.Options)) (*sqs.ListDeadLetterSourceQueuesOutput, error) {
	return c.SqsClient.ListDeadLetterSourceQueues(ctx, params, optFns...)
}

func (c *SqsExtendedClient) ListQueues(ctx context.Context, params *sqs.ListQueuesInput, optFns ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error) {
	return c.SqsClient.ListQueues(ctx, params, optFns...)
}

func (c *SqsExtendedClient) ListQueueTags(ctx context.Context, params *sqs.ListQueueTagsInput, optFns ...func(*sqs.Options)) (*sqs.ListQueueTagsOutput, error) {
	return c.SqsClient.ListQueueTags(ctx, params, optFns...)
}

func (c *SqsExtendedClient) PurgeQueue(ctx context.Context, params *sqs.PurgeQueueInput, optFns ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error) {
	return c.SqsClient.PurgeQueue(ctx, params, optFns...)
}

// Retrieves one or more messages, with a maximum limit of 10 messages, from the specified queue.
// Downloads the message payloads from Amazon S3 when necessary.
// Long poll support is enabled by using the WaitTimeSeconds parameter.
// For more information, see http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html.
func (c *SqsExtendedClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if params == nil {
		err := errors.New("ReceiveMessageInput cannot be nil")
		log.Println(err)
		return nil, err
	}
	middleware.AddUserAgentKeyValue("USER_AGENT", appconst.UserAgentHdr)
	if !c.SqsClientConfig.PayloadSupport {
		return c.SqsClient.ReceiveMessage(ctx, params, optFns...)
	}

	params.MessageAttributeNames = appconst.ReservedAttrNames
	output, err := c.SqsClient.ReceiveMessage(ctx, params, optFns...)
	if err != nil {
		return nil, err
	}

	msgs := output.Messages
	for _, msg := range msgs {
		// for each received message check if they are stored in S3
		if c.getReservedAttrNameIfPresent(msg.MessageAttributes) != "" {
			largeMsgPointer := *msg.Body
			// TODO: Test if old string is really the one in the Body
			largeMsgPointer = strings.Replace(largeMsgPointer, "com.amazon.sqs.javamessaging.MessageS3Pointer", "github.com.threehook.aws-payload-offloading-go.payload.PayloadS3Pointer", 1)
			pl, err := c.PayloadStore.GetOriginalPayload(largeMsgPointer)
			if err != nil {
				return nil, err
			}
			*msg.Body = pl

			// Remove the additional attribute before returning the message to user
			for _, attrName := range appconst.ReservedAttrNames {
				delete(msg.MessageAttributes, attrName)
			}

			// Embed s3 object pointer in the receipt handle
			modifiedReceiptHandle, err := c.embedS3PointerInReceiptHandle(*msg.ReceiptHandle, largeMsgPointer)
			if err != nil {
				return nil, err
			}
			msg.ReceiptHandle = &modifiedReceiptHandle
		}
	}
	return output, nil
}

func (c *SqsExtendedClient) RemovePermission(ctx context.Context, params *sqs.RemovePermissionInput, optFns ...func(*sqs.Options)) (*sqs.RemovePermissionOutput, error) {
	return c.SqsClient.RemovePermission(ctx, params, optFns...)
}

// Delivers a message to the specified queue and uploads the message payload to Amazon S3 if necessary.
func (c *SqsExtendedClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(options *sqs.Options)) (*sqs.SendMessageOutput, error) {
	if params == nil {
		err := errors.New("SendMessageInput cannot be nil")
		log.Println(err)
		return nil, err
	}
	middleware.AddUserAgentKeyValue("USER_AGENT", appconst.UserAgentHdr)
	if !c.SqsClientConfig.PayloadSupport {
		return c.SqsClient.SendMessage(ctx, params, optFns...)
	}

	if params.MessageBody == nil || *params.MessageBody == "" {
		err := errors.New("MessageBody cannot be nil or empty")
		log.Println(err)
		return nil, err
	}

	// Check message attributes for ExtendedClient related constraints
	c.checkMessageAttrs(params.MessageAttributes)
	if c.SqsClientConfig.AlwaysThroughS3 || c.isLarge(params) {
		var err error
		params, err = c.storeMessageInS3(params)
		if err != nil {
			return nil, err
		}
	}
	return c.SqsClient.SendMessage(ctx, params, optFns...)

}

// Delivers up to ten messages to the specified queue. This is a batch version of SendMessage.
// The result of the send action on each message is reported individually in the response.
// Uploads message payloads to Amazon S3 when necessary.
func (c *SqsExtendedClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	if params == nil {
		err := errors.New("SendMessageBatchInput cannot be nil")
		log.Println(err)
		return nil, err
	}
	middleware.AddUserAgentKeyValue("USER_AGENT", appconst.UserAgentHdr)
	if !c.SqsClientConfig.PayloadSupport {
		return c.SqsClient.SendMessageBatch(ctx, params, optFns...)
	}

	batchEntries := params.Entries
	index := 0
	for _, entry := range params.Entries {
		// Check message attributes for ExtendedClient related constraints
		if err := c.checkMessageAttrs(entry.MessageAttributes); err != nil {
			return nil, err
		}
		if c.SqsClientConfig.AlwaysThroughS3 || c.isLargeForBatchEntry(&entry) {
			batchEntry, err := c.storeMessageBatchInS3(&entry)
			if err != nil {
				return nil, err
			}
			batchEntries[index] = *batchEntry
		}
		index++
	}

	return c.SqsClient.SendMessageBatch(ctx, params, optFns...)
}

func (c *SqsExtendedClient) SetQueueAttributes(ctx context.Context, params *sqs.SetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error) {
	return c.SqsClient.SetQueueAttributes(ctx, params, optFns...)
}

func (c *SqsExtendedClient) TagQueue(ctx context.Context, params *sqs.TagQueueInput, optFns ...func(*sqs.Options)) (*sqs.TagQueueOutput, error) {
	return c.SqsClient.TagQueue(ctx, params, optFns...)
}

func (c *SqsExtendedClient) UntagQueue(ctx context.Context, params *sqs.UntagQueueInput, optFns ...func(*sqs.Options)) (*sqs.UntagQueueOutput, error) {
	return c.SqsClient.UntagQueue(ctx, params, optFns...)
}

func (c *SqsExtendedClient) checkMessageAttrs(msgAttrs map[string]types.MessageAttributeValue) error {
	msgAttrSize := c.getMsgAttrSize(msgAttrs)
	if msgAttrSize > c.SqsClientConfig.PayloadSizeThreshold {
		err := fmt.Errorf("Total size of Message attributes is %d bytes which is larger than the threshold of"+
			" %d bytes. Consider including the payload in the message body instead of message attributes", msgAttrSize,
			c.SqsClientConfig.PayloadSizeThreshold)
		log.Println(err)
		return err
	}
	msgAttrNum := len(msgAttrs)
	if msgAttrNum > appconst.MaxAllowedAttributes {
		err := fmt.Errorf("Number of message attributes [%d] exceeds the maximum allowed for large-payload"+
			" messages [%d].", msgAttrNum, appconst.MaxAllowedAttributes)
		log.Println(err)
		return err
	}
	largePayloadAttrName := c.getReservedAttrNameIfPresent(msgAttrs)
	if largePayloadAttrName != "" {
		err := fmt.Errorf("Message attribute name %s is reserved for use by SQS extended client", largePayloadAttrName)
		log.Println(err)
		return err
	}
	return nil
}

func (c *SqsExtendedClient) getMsgAttrSize(msgAttrs map[string]types.MessageAttributeValue) int32 {
	totalMsgAttrSize := int32(0)

	for key, attr := range msgAttrs {
		totalMsgAttrSize += util.GetStringSizeInBytes(key)
		if attr.DataType != nil {
			totalMsgAttrSize += util.GetStringSizeInBytes(*attr.DataType)
		}

		stringVal := attr.StringValue
		if stringVal != nil {
			totalMsgAttrSize += util.GetStringSizeInBytes(*stringVal)
		}

		binVal := attr.BinaryValue
		if binVal != nil {
			totalMsgAttrSize += int32(len(binVal))
		}
	}
	return totalMsgAttrSize
}

func (c *SqsExtendedClient) getReservedAttrNameIfPresent(msgAttrs map[string]types.MessageAttributeValue) string {
	var reservedAttrName = ""
	if _, ok := msgAttrs[appconst.ReservedAttrName]; ok {
		reservedAttrName = appconst.ReservedAttrName
	} else if _, ok := msgAttrs[appconst.LegacyReservedAttrName]; ok {
		reservedAttrName = appconst.LegacyReservedAttrName
	}
	return reservedAttrName
}

func (c *SqsExtendedClient) isLarge(input *sqs.SendMessageInput) bool {
	msgAttrSize := c.getMsgAttrSize(input.MessageAttributes)
	msgBodySize := util.GetStringSizeInBytes(*input.MessageBody)
	totalMsgSize := msgAttrSize + msgBodySize
	return totalMsgSize > c.SqsClientConfig.PayloadSizeThreshold
}

func (c *SqsExtendedClient) isLargeForBatchEntry(entry *types.SendMessageBatchRequestEntry) bool {
	msgAttrSize := c.getMsgAttrSize(entry.MessageAttributes)
	msgBodySize := util.GetStringSizeInBytes(*entry.MessageBody)
	totalMsgSize := msgAttrSize + msgBodySize
	return totalMsgSize > c.SqsClientConfig.PayloadSizeThreshold
}

func (c *SqsExtendedClient) storeMessageBatchInS3(batchEntry *types.SendMessageBatchRequestEntry) (*types.SendMessageBatchRequestEntry, error) {
	// Read the content of the message from message body
	msgContentStr := batchEntry.MessageBody
	msgContentSize := util.GetStringSizeInBytes(*msgContentStr)

	// Add a new message attribute as a flag
	dataType := "Number"
	stringValue := strconv.Itoa(int(msgContentSize))
	msgAttrValue := &types.MessageAttributeValue{DataType: &dataType, StringValue: &stringValue}

	// TODO: Hmm could map creation be earlier?
	if batchEntry.MessageAttributes == nil {
		batchEntry.MessageAttributes = make(map[string]types.MessageAttributeValue)
	}
	if !c.SqsClientConfig.UseLegacyReservedAttributeName {
		batchEntry.MessageAttributes[appconst.ReservedAttrName] = *msgAttrValue
	} else {
		batchEntry.MessageAttributes[appconst.LegacyReservedAttrName] = *msgAttrValue
	}

	// Store the message content in S3.
	largeMsgPointer, err := c.PayloadStore.StoreOriginalPayload(*msgContentStr)
	if err != nil {
		return nil, err
	}
	batchEntry.MessageBody = &largeMsgPointer

	return batchEntry, nil
}

func (c *SqsExtendedClient) storeMessageInS3(input *sqs.SendMessageInput) (*sqs.SendMessageInput, error) {
	// Read the content of the message from message body
	msgContentStr := input.MessageBody
	msgContentSize := util.GetStringSizeInBytes(*msgContentStr)

	// Add a new message attribute as a flag
	dataType := "Number"
	stringValue := strconv.Itoa(int(msgContentSize))
	msgAttrValue := types.MessageAttributeValue{DataType: &dataType, StringValue: &stringValue}

	if input.MessageAttributes == nil {
		input.MessageAttributes = make(map[string]types.MessageAttributeValue)
	}
	msgAttrValueMap := input.MessageAttributes
	if !c.SqsClientConfig.UseLegacyReservedAttributeName {
		msgAttrValueMap[appconst.ReservedAttrName] = msgAttrValue
	} else {
		msgAttrValueMap[appconst.LegacyReservedAttrName] = msgAttrValue
	}

	// Store the message content in S3.
	s3Pointer, err := c.PayloadStore.StoreOriginalPayload(*msgContentStr)
	if err != nil {
		return nil, err
	}
	input.MessageBody = &s3Pointer

	return input, nil
}

// TODO: Wrap the message pointer as-is to the receiptHandle so that it can be generic and does not use any
// LargeMessageStore implementation specific details.
func (c *SqsExtendedClient) embedS3PointerInReceiptHandle(receiptHandle, pointer string) (string, error) {
	s3Pointer, err := payload.FromJson(pointer)
	if err != nil {
		return "", err
	}
	s3BucketName := s3Pointer.S3BucketName
	s3Key := s3Pointer.S3Key

	bmrkr := appconst.S3BucketNameMarker
	kmrkr := appconst.S3KeyMarker

	return bmrkr + s3BucketName + bmrkr + kmrkr + s3Key + kmrkr + receiptHandle, nil
}

func (c *SqsExtendedClient) isS3ReceiptHandle(receiptHandle string) bool {
	return strings.Contains(receiptHandle, appconst.S3BucketNameMarker) && strings.Contains(receiptHandle, appconst.S3KeyMarker)
}

func (c *SqsExtendedClient) getOrigReceiptHandle(receiptHandle string) string {
	index1 := strings.Index(receiptHandle, appconst.S3KeyMarker)
	index2 := util.FindIndex(receiptHandle, appconst.S3KeyMarker, index1+len(appconst.S3KeyMarker))
	return receiptHandle[index2+len(appconst.S3KeyMarker):]
}

func (c *SqsExtendedClient) getMessagePointerFromModifiedReceiptHandle(receiptHandle string) (string, error) {
	s3MsgBucketName := c.getFromReceiptHandleByMarker(receiptHandle, appconst.S3BucketNameMarker)
	s3MsgKey := c.getFromReceiptHandleByMarker(receiptHandle, appconst.S3KeyMarker)
	payloadS3Pointer := payload.PayloadS3Pointer{s3MsgBucketName, s3MsgKey}
	json, err := payloadS3Pointer.ToJson()
	if err != nil {
		return "", err
	}
	return json, nil
}

func (c *SqsExtendedClient) getFromReceiptHandleByMarker(receiptHandle, marker string) string {
	firstOccurence := strings.Index(receiptHandle, marker)
	secondOccurence := util.FindIndex(receiptHandle, marker, firstOccurence+1)

	return receiptHandle[firstOccurence+len(marker) : secondOccurence]
}
