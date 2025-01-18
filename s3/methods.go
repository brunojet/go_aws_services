package s3

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3ServiceInterface define a interface para as funções S3.
type S3ServiceInterface interface {
	PreSign(req *request.Request, expire time.Duration) (string, error)
	PutObjectRequest(metadata Metadata) (PresignedUrlResponse, error)
	GenerateSignedRequest(metadata Metadata) (PresignedUrlResponse, error)
}

// S3Service é uma implementação da interface S3ServiceInterface.
type S3Service struct{}

func (s *S3Service) PreSign(req *request.Request, expire time.Duration) (string, error) {
	return req.Presign(expire)

}

func (s *S3Service) PutObjectRequest(metadata Metadata) (*request.Request, *s3.PutObjectOutput) {
	s3Client := initAwsS3()

	return s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String("brunojet-storage"),
		Key:    aws.String(fmt.Sprintf("uploads/%d-%d-%d.apk", metadata.PartnerID, metadata.AppID, metadata.DeviceModelID)),
	})

}

func (s *S3Service) GenerateSignedRequest(metadata Metadata) (PresignedUrlResponse, error) {
	req, _ := s.PutObjectRequest(metadata)
	urlStr, err := s.PreSign(req, 15*time.Minute)

	if err != nil {
		return PresignedUrlResponse{}, fmt.Errorf("failed to sign request: %v", err)
	}

	return PresignedUrlResponse{
		ID:           metadata.AppID,
		PresignedUrl: urlStr,
	}, nil
}
