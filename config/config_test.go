package config

import (
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/threehook/aws-payload-offloading-go/encryption"
	s3mocks "github.com/threehook/aws-payload-offloading-go/mocks"
	"github.com/threehook/aws-sqs-extended-client-go/appconst"
	"github.com/threehook/aws-sqs-extended-client-go/mocks"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

const (
	s3BucketName                   = "test-bucket-name"
	s3ServerSideEncryptionKmsKeyId = "test-customer-managed-kms-key-id"
)

var (
	m mcks
)

type mcks struct {
	ctrl             *gomock.Controller
	mockS3Client     *s3mocks.MockS3SvcClientI
	mockSqsExtClient *mocks.MockSqsSvcClientI
}

func beforeEach(t *testing.T) {
	ctrl := gomock.NewController(t)
	m = mcks{
		ctrl:             ctrl,
		mockS3Client:     s3mocks.NewMockS3SvcClientI(ctrl),
		mockSqsExtClient: mocks.NewMockSqsSvcClientI(ctrl),
	}
}

func TestMain(m *testing.M) {
	// Suppress logging in unit tests
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}

func TestCopyConstructor(t *testing.T) {
	beforeEach(t)
	alwaysThroughS3 := true
	msgSizeThreshold := int32(500)
	cleanupS3Payload := false
	cfg, _ := NewExtendedClientConfigBuilder().
		WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		WithAlwaysThroughS3(alwaysThroughS3).
		WithCleanupS3Payload(false).
		WithPayloadSizeThreshold(msgSizeThreshold).
		WithServerSideEncryptionStrategy(&encryption.CustomerKey{AwsKmsKeyId: s3ServerSideEncryptionKmsKeyId}).Build()

	newCfg := NewExtendedClientConfigurationfromOther(*cfg)

	assert.Equal(t, m.mockS3Client, newCfg.S3Client)
	assert.Equal(t, s3BucketName, newCfg.S3BucketName)
	assert.Equal(t, s3ServerSideEncryptionKmsKeyId, newCfg.ServerSideEncryptionStrategy.(*encryption.CustomerKey).AwsKmsKeyId)
	assert.True(t, newCfg.PayloadSupport)
	assert.Equal(t, cleanupS3Payload, newCfg.CleanupS3Payload)
	assert.Equal(t, alwaysThroughS3, newCfg.AlwaysThroughS3)
	assert.Equal(t, msgSizeThreshold, newCfg.PayloadSizeThreshold)
	assert.NotSame(t, newCfg, cfg)
}

func TestLargePayloadSupportEnabledWithDefaultDeleteFromS3Config(t *testing.T) {
	beforeEach(t)
	cfg, _ := NewExtendedClientConfigBuilder().
		WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		Build()

	assert.True(t, cfg.PayloadSupport)
	assert.True(t, cfg.CleanupS3Payload)
	assert.NotNil(t, cfg.S3Client)
	assert.Equal(t, s3BucketName, cfg.S3BucketName)
}

func TestLargePayloadSupportEnabledWithDeleteFromS3Enabled(t *testing.T) {
	beforeEach(t)
	cfg, _ := NewExtendedClientConfigBuilder().
		WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		WithCleanupS3Payload(true).
		Build()

	assert.True(t, cfg.PayloadSupport)
	assert.True(t, cfg.CleanupS3Payload)
	assert.NotNil(t, cfg.S3Client)
	assert.Equal(t, s3BucketName, cfg.S3BucketName)
}

func TestLargePayloadSupportEnabledWithDeleteFromS3Disabled(t *testing.T) {
	beforeEach(t)
	cfg, _ := NewExtendedClientConfigBuilder().
		WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		WithCleanupS3Payload(false).
		Build()

	assert.True(t, cfg.PayloadSupport)
	assert.False(t, cfg.CleanupS3Payload)
	assert.NotNil(t, cfg.S3Client)
	assert.Equal(t, s3BucketName, cfg.S3BucketName)
}

func TestLargePayloadSupportEnabled(t *testing.T) {
	beforeEach(t)
	cfg, _ := NewExtendedClientConfigBuilder().
		WithPayloadSupportEnabled(m.mockS3Client, s3BucketName).
		Build()

	assert.True(t, cfg.PayloadSupport)
	assert.NotNil(t, cfg.S3Client)
	assert.Equal(t, s3BucketName, cfg.S3BucketName)
}

func TestDisableLargePayloadSupport(t *testing.T) {
	beforeEach(t)
	cfg, _ := NewExtendedClientConfigBuilder().
		WithPayloadSupportDisabled().
		Build()

	assert.Nil(t, cfg.S3Client)
	assert.Empty(t, cfg.S3BucketName)
}

func TestMessageSizeThreshold(t *testing.T) {
	beforeEach(t)
	cfg, _ := NewExtendedClientConfigBuilder().
		Build()

	assert.Equal(t, appconst.DefaultMessageSizeThreshold, cfg.PayloadSizeThreshold)
	msgLength := int32(1000)
	cfg.PayloadSizeThreshold = msgLength
	assert.Equal(t, msgLength, cfg.PayloadSizeThreshold)

}
