package dynamodb

import (
	"errors"
	"go_aws_services/session"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoDBService interface {
	PutItem(item map[string]*dynamodb.AttributeValue) (*dynamodb.PutItemOutput, error)
	QueryItem(key map[string]interface{}, indexName string) (*dynamodb.QueryOutput, error)
	GetItem(key map[string]*dynamodb.AttributeValue) (*dynamodb.GetItemOutput, error)
	DeleteItem(key map[string]*dynamodb.AttributeValue) (*dynamodb.DeleteItemOutput, error)
	CreateTableAsync() (*dynamodb.CreateTableOutput, error)
	CreateTable() (*dynamodb.CreateTableOutput, error)
	DeleteTableAsync() (*dynamodb.DeleteTableOutput, error)
	DeleteTable() (*dynamodb.DeleteTableOutput, error)
}

func NewDynamoDBClient(tableName string, keySchemaInput KeySchemaInput, gsiKeySchemaInput []*GsiKeySchemaInput) *DynamoDBClient {
	sess := session.GetAWSSession()
	return &DynamoDBClient{
		client:       dynamodb.New(sess),
		tableName:    tableName,
		keySchema:    keySchemaInput,
		gsiKeySchema: gsiKeySchemaInput,
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
