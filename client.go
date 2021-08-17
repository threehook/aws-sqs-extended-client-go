package aws_sqs_extended_client_go

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/threehook/aws-payload-offloading-go/payload"
	"github.com/threehook/aws-payload-offloading-go/s3"
)

type SqsSvcClientI interface {
	AddPermission(ctx context.Context, params *sqs.AddPermissionInput, optFns ...func(*sqs.Options)) (*sqs.AddPermissionOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
	CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	DeleteQueue(ctx context.Context, params *sqs.DeleteQueueInput, optFns ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error)
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	ListDeadLetterSourceQueues(ctx context.Context, params *sqs.ListDeadLetterSourceQueuesInput, optFns ...func(*sqs.Options)) (*sqs.ListDeadLetterSourceQueuesOutput, error)
	ListQueues(ctx context.Context, params *sqs.ListQueuesInput, optFns ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error)
	ListQueueTags(ctx context.Context, params *sqs.ListQueueTagsInput, optFns ...func(*sqs.Options)) (*sqs.ListQueueTagsOutput, error)
	PurgeQueue(ctx context.Context, params *sqs.PurgeQueueInput, optFns ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	RemovePermission(ctx context.Context, params *sqs.RemovePermissionInput, optFns ...func(*sqs.Options)) (*sqs.RemovePermissionOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	SetQueueAttributes(ctx context.Context, params *sqs.SetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error)
	TagQueue(ctx context.Context, params *sqs.TagQueueInput, optFns ...func(*sqs.Options)) (*sqs.TagQueueOutput, error)
	UntagQueue(ctx context.Context, params *sqs.UntagQueueInput, optFns ...func(*sqs.Options)) (*sqs.UntagQueueOutput, error)
}

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
		S3Client:             config.S3Client,
		ServerSideEncryption: config.ServerSideEncryption,
		ObjectCannedACL:      config.ObjectCannedACL,
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

func (c *SqsExtendedClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return c.SqsClient.DeleteMessage(ctx, params, optFns...)
}

func (c *SqsExtendedClient) DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
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

func (c *SqsExtendedClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return c.SqsClient.ReceiveMessage(ctx, params, optFns...)
}

func (c *SqsExtendedClient) RemovePermission(ctx context.Context, params *sqs.RemovePermissionInput, optFns ...func(*sqs.Options)) (*sqs.RemovePermissionOutput, error) {
	return c.SqsClient.RemovePermission(ctx, params, optFns...)
}

func (c *SqsExtendedClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return c.SqsClient.SendMessage(ctx, params, optFns...)
}

func (c *SqsExtendedClient) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
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
