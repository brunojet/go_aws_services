package dynamodb

import (
	"errors"
	"go_aws_services/session"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	getAwsSession                           = session.GetAWSSession
	newDynamodb                             = dynamodb.New
	dynamoClient  dynamodbiface.DynamoDBAPI = nil
)

func initAwsDynamoDb() dynamodbiface.DynamoDBAPI {
	if dynamoClient == nil {
		dynamoClient = newDynamodb(getAwsSession())
		if dynamoClient == nil {
			panic(errors.New("failed to create dynamodb"))
		}
	}

	return dynamoClient
}
