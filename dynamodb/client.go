package dynamodb

import (
	"go_aws_services/session"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	dynamoClient dynamodbiface.DynamoDBAPI = nil
	once         sync.Once
)

func initAwsDynamoDb() dynamodbiface.DynamoDBAPI {
	once.Do(func() {
		if dynamoClient == nil {
			dynamoClient = dynamodb.New(session.GetAWSSession())
		}
		if dynamoClient == nil {
			panic("failed to create dynamodb")
		}
	})

	return dynamoClient
}
