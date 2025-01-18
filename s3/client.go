package s3

import (
	"errors"
	"go_aws_services/session"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	s3Client s3iface.S3API = nil
)

func initAwsS3() s3iface.S3API {
	if s3Client == nil {
		s3Client = s3.New(session.GetAWSSession())
		if s3Client == nil {
			panic(errors.New("failed to create s3 client"))
		}
	}

	return s3Client
}
