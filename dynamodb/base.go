package dynamodb

import (
	"errors"
	custom_session "go_aws_services/session"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var newSession = custom_session.GetAWSSession
var newDynamodb = dynamodb.New

var _ DynamoDBService = (*DynamoDBClient)(nil)

type DynamoDBService interface {
	PutItem(item map[string]interface{}) (*dynamodb.PutItemOutput, error)
	QueryItem(key map[string]interface{}, indexName string) (*dynamodb.QueryOutput, error)
	GetItem(key map[string]interface{}) (*dynamodb.GetItemOutput, error)
	CreateTableAsync() (*dynamodb.CreateTableOutput, error)
	CreateTable() (*dynamodb.CreateTableOutput, error)
	DeleteTableAsync() (*dynamodb.DeleteTableOutput, error)
	DeleteTable() (*dynamodb.DeleteTableOutput, error)
}

func NewDynamoDBClient(tableName string, keySchemaInput KeySchemaInput, gsiKeySchemaInput []*GsiKeySchemaInput) *DynamoDBClient {
	session := newSession()
	client := newDynamodb(session)
	return &DynamoDBClient{
		tableName:    tableName,
		keySchema:    keySchemaInput,
		gsiKeySchema: gsiKeySchemaInput,
		client:       client,
	}
}

func findGsiKeySchema(gsiKeySchema []*GsiKeySchemaInput, indexName string) (*GsiKeySchemaInput, error) {
	for _, gsi := range gsiKeySchema {
		if gsi.IndexName == indexName {
			return gsi, nil
		}
	}
	return nil, errors.New("GSI index not found")
}

func buildKeyConditionExpression(gsiKeySchema *GsiKeySchemaInput, key map[string]interface{}) (string, map[string]*string, map[string]*dynamodb.AttributeValue) {
	expressionAttributeNames := make(map[string]*string)
	expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)

	keyConditionExpression := ""

	if gsiKeySchema.HashKey != "" {
		expressionAttributeNames["#hk"] = aws.String(gsiKeySchema.HashKey)
		expressionAttributeValues[":hk"] = &dynamodb.AttributeValue{
			S: aws.String(key[gsiKeySchema.HashKey].(string)),
		}
		keyConditionExpression = "#hk = :hk"
	}

	if gsiKeySchema.RangeKey != "" {
		expressionAttributeNames["#rk"] = aws.String(gsiKeySchema.RangeKey)
		expressionAttributeValues[":rk"] = &dynamodb.AttributeValue{
			S: aws.String(key[gsiKeySchema.RangeKey].(string)),
		}
		if keyConditionExpression != "" {
			keyConditionExpression += " AND "
		}
		keyConditionExpression += "#rk = :rk"
	}

	return keyConditionExpression, expressionAttributeNames, expressionAttributeValues
}
