package appconst

import "github.com/threehook/aws-payload-offloading-go/util"

const (
	clientName = "aws-sqs-extended-client-go"

	LegacyReservedAttrName = "SQSLargePayloadSize"

	// This constant is shared with SNSExtendedClient
	// SNS team should be notified of any changes made to this
	ReservedAttrName = "ExtendedPayloadSize"

	// This constant is shared with SNSExtendedClient
	// SNS team should be notified of any changes made to this
	MaxAllowedAttributes = 10 - 1

	// This constant is shared with SNSExtendedClient
	// SNS team should be notified of any changes made to this
	DefaultMessageSizeThreshold = int32(262144)

	S3BucketNameMarker = "-..s3BucketName..-"
	S3KeyMarker        = "-..s3Key..-"
)

var (
	UserAgentHdr      = util.GetUserAgentHeader(clientName)
	ReservedAttrNames = []string{LegacyReservedAttrName, ReservedAttrName}
)
