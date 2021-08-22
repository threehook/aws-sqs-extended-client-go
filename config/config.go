package config

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	poconf "github.com/threehook/aws-payload-offloading-go/config"
	"github.com/threehook/aws-payload-offloading-go/encryption"
	"github.com/threehook/aws-payload-offloading-go/s3"
	"github.com/threehook/aws-sqs-extended-client-go/appconst"
	"log"
)

// Amazon SQS extended client configuration options such as Amazon S3 client, bucket name, and message size threshold
// for large-payload messages
type ExtendedClientConfig struct {
	poconf.PayloadStorageConfig
	CleanupS3Payload               bool
	UseLegacyReservedAttributeName bool
}

func NewExtendedClientConfigurationfromOther(other ExtendedClientConfig) *ExtendedClientConfig {
	otherPayloadStorageConfig := &poconf.PayloadStorageConfig{
		S3Client:                     other.S3Client,
		S3BucketName:                 other.S3BucketName,
		PayloadSizeThreshold:         other.PayloadSizeThreshold,
		AlwaysThroughS3:              other.AlwaysThroughS3,
		PayloadSupport:               other.PayloadSupport,
		ServerSideEncryptionStrategy: other.ServerSideEncryptionStrategy,
		ObjectCannedACL:              other.ObjectCannedACL,
	}

	return &ExtendedClientConfig{
		PayloadStorageConfig:           *otherPayloadStorageConfig,
		CleanupS3Payload:               other.CleanupS3Payload,
		UseLegacyReservedAttributeName: other.UseLegacyReservedAttributeName,
	}
}

type ExtendedClientConfigBuilder struct {
	ExtendedClientConfig ExtendedClientConfig
}

func NewExtendedClientConfigBuilder() *ExtendedClientConfigBuilder {
	extClientConfig := ExtendedClientConfig{
		PayloadStorageConfig:           poconf.PayloadStorageConfig{PayloadSizeThreshold: appconst.DefaultMessageSizeThreshold},
		CleanupS3Payload:               true,
		UseLegacyReservedAttributeName: true,
	}
	return &ExtendedClientConfigBuilder{extClientConfig}
}

func (b *ExtendedClientConfigBuilder) WithPayloadSupportEnabled(s3Client s3.S3SvcClientI, s3BucketName string) *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.S3Client = s3Client
	b.ExtendedClientConfig.S3BucketName = s3BucketName
	b.ExtendedClientConfig.PayloadSupport = true
	return b
}

func (b *ExtendedClientConfigBuilder) WithPayloadSupportDisabled() *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.S3Client = nil
	b.ExtendedClientConfig.S3BucketName = ""
	b.ExtendedClientConfig.PayloadSupport = false
	return b
}

func (b *ExtendedClientConfigBuilder) WithAlwaysThroughS3(alwaysThroughS3 bool) *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.AlwaysThroughS3 = alwaysThroughS3
	return b
}

func (b *ExtendedClientConfigBuilder) WithPayloadSizeThreshold(payloadSizeThreshold int32) *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.PayloadSizeThreshold = payloadSizeThreshold
	return b
}

func (b *ExtendedClientConfigBuilder) WithServerSideEncryptionStrategy(strategy encryption.ServerSideEncryptionStrategy) *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.ServerSideEncryptionStrategy = strategy
	return b
}

func (b *ExtendedClientConfigBuilder) WithCleanupS3Payload(cleanup bool) *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.CleanupS3Payload = cleanup
	return b
}

func (b *ExtendedClientConfigBuilder) WithUseLegacyReservedAttributeName(legacy bool) *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.UseLegacyReservedAttributeName = legacy
	return b
}

func (b *ExtendedClientConfigBuilder) WithCannedAccessControlList(ObjectCannedACL types.ObjectCannedACL) *ExtendedClientConfigBuilder {
	b.ExtendedClientConfig.ObjectCannedACL = ObjectCannedACL
	return b
}

func (b *ExtendedClientConfigBuilder) Build() (*ExtendedClientConfig, error) {
	ecc := &b.ExtendedClientConfig

	if ecc.PayloadSupport && ecc.S3Client != nil && ecc.S3BucketName != "" {
		log.Println("Payload support is already enabled. Overwriting AmazonS3Client and S3BucketName.") // warn
	}
	if ecc.PayloadSupport && (ecc.S3Client == nil || ecc.S3BucketName == "") {
		err := errors.New("S3 client and/or S3 bucket name cannot be null.")
		log.Println(err)
		return nil, err
	}

	var payloadSupportAction string
	if ecc.PayloadSupport {
		payloadSupportAction = "enabled"
	} else {
		payloadSupportAction = "disabled"
	}
	log.Printf("Payload support %s\n", payloadSupportAction)                // info
	log.Printf("Payload always sent through S3: %t\n", ecc.AlwaysThroughS3) // info
	log.Printf("Payload threshold is %d\n", ecc.PayloadSizeThreshold)       // info

	var ssEncr string
	switch ecc.ServerSideEncryptionStrategy.(type) {
	case *encryption.CustomerKey:
		ssEncr = fmt.Sprintf("%s with CustomerId\n", types.ServerSideEncryptionAwsKms)
	case *encryption.AwsManagedCmk:
		ssEncr = fmt.Sprintf("%s\n", types.ServerSideEncryptionAwsKms)
	default:
		ssEncr = "none"
	}

	log.Printf("Payload serverSide encryption is %s\n", ssEncr)                                  // info
	log.Printf("Cleanup S3 payload: %t\n", ecc.CleanupS3Payload)                                 // info
	log.Printf("Use Legacy Reserved attribute name: %t\n\n", ecc.UseLegacyReservedAttributeName) // info

	return ecc, nil
}

//psc.S3Client = nil
//psc.S3BucketName = ""
//psc.PayloadSupport = false
//log.Println("Payload support disabled.") // info

// SetPayloadSupportEnabled enables support for payload messages
//func (ecc *ExtendedClientConfig) SetPayloadSupportEnabled(s3Client s3.S3SvcClientI, s3BucketName string) error {
//	if s3Client != nil && &s3BucketName != nil {
//		if ecc.PayloadSupport {
//			log.Println("Payload support is already enabled. Overwriting AmazonS3Client and S3BucketName.") // warn
//		}
//		ecc.S3Client = s3Client
//		ecc.S3BucketName = s3BucketName
//		ecc.PayloadSupport = true
//		log.Println("Payload support enabled.") // info
//	} else {
//		err := errors.New("S3 client and/or S3 bucket name cannot be null.")
//		log.Println(err)
//		return err
//	}
//	ecc.S3Client = s3Client
//	ecc.S3BucketName = s3BucketName
//
//	return nil
//}

//func (ecc *ExtendedClientConfig) WithPayloadSupportEnabled(s3Client s3.S3SvcClientI, s3BucketName string) (*ExtendedClientConfig, error) {
//	if err := ecc.SetPayloadSupportEnabled(s3Client, s3BucketName); err != nil {
//		return nil, err
//	}
//	return ecc, nil
//}

//func (ecc *ExtendedClientConfig) WithPayloadSupportDisabled() *ExtendedClientConfig {
//	ecc.SetPayloadSupportDisabled()
//	return ecc
//}
