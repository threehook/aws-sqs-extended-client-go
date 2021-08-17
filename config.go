package aws_sqs_extended_client_go

import (
	"errors"
	poconf "github.com/threehook/aws-payload-offloading-go/config"
	po "github.com/threehook/aws-payload-offloading-go/s3"
	"log"
)

const (
	// This constant is shared with SNSExtendedClient
	// SNS team should be notified of any changes made to this
	reservedAttributeName = "ExtendedPayloadSize"

	// This constant is shared with SNSExtendedClient
	// SNS team should be notified of any changes made to this
	maxAllowedAttributes = 10 - 1

	// This constant is shared with SNSExtendedClient
	// SNS team should be notified of any changes made to this
	defaultMessageSizeThreshold = 262144

	s3BucketNameMarker = "-..s3BucketName..-"
	s3KeyMarker        = "-..s3Key..-"
)

// Amazon SQS extended client configuration options such as Amazon S3 client, bucket name, and message size threshold
// for large-payload messages
type ExtendedClientConfig struct {
	poconf.PayloadStorageConfig
	CleanupS3Payload               bool
	UseLegacyReservedAttributeName bool
}

func NewExtendedClientConfiguration() *ExtendedClientConfig {
	return &ExtendedClientConfig{
		CleanupS3Payload:               true,
		UseLegacyReservedAttributeName: true,
	}
}

func NewExtendedClientConfigurationfromOther(other ExtendedClientConfig) *ExtendedClientConfig {
	return &ExtendedClientConfig{
		CleanupS3Payload:               other.CleanupS3Payload,
		UseLegacyReservedAttributeName: other.UseLegacyReservedAttributeName,
	}
}

// SetPayloadSupportEnabled enables support for payload messages
func (ecc *ExtendedClientConfig) SetPayloadSupportEnabled(s3Client po.S3SvcClientI, s3BucketName string) error {
	if s3Client != nil && &s3BucketName != nil {
		if ecc.PayloadSupport {
			log.Println("Payload support is already enabled. Overwriting AmazonS3Client and S3BucketName.") // warn
		}
		ecc.S3Client = s3Client
		ecc.S3BucketName = s3BucketName
		ecc.PayloadSupport = true
		log.Println("Payload support enabled.") // info
	} else {
		err := errors.New("S3 client and/or S3 bucket name cannot be null.")
		log.Println(err)
		return err
	}
	ecc.S3Client = s3Client
	ecc.S3BucketName = s3BucketName

	return nil
}

func (ecc *ExtendedClientConfig) WithPayloadSupportEnabled(s3Client po.S3SvcClientI, s3BucketName string) (*ExtendedClientConfig, error) {
	if err := ecc.SetPayloadSupportEnabled(s3Client, s3BucketName); err != nil {
		return nil, err
	}
	return ecc, nil
}
