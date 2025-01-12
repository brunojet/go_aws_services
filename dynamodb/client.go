package dynamodb

import (
	"go_aws_services/session"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	dynamoClient dynamodbiface.DynamoDBAPI = &dynamodb.DynamoDB{}
	isInitiated  bool                      = false
)

func initAwsDynamoDb() {
	if !isInitiated {
		dynamoClient := dynamodb.New(session.GetAWSSession())
		if dynamoClient == nil {
			panic("failed to create dynamodb")
		}
		isInitiated = true
	}
}
