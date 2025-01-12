package dynamodb

import (
	"errors"
	"go_aws_services/session"

	"github.com/aws/aws-sdk-go/aws"
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

// var _ DynamoDBService = (*DynamoDBClient)(nil)

type DynamoDBService interface {
	PutItem(item map[string]interface{}) (*dynamodb.PutItemOutput, error)
	QueryItem(key map[string]interface{}, indexName string) (*dynamodb.QueryOutput, error)
	GetItem(key map[string]interface{}) (*dynamodb.GetItemOutput, error)
	CreateTableAsync() (*dynamodb.CreateTableOutput, error)
	CreateTable() (*dynamodb.CreateTableOutput, error)
	DeleteTableAsync() (*dynamodb.DeleteTableOutput, error)
	DeleteTable() (*dynamodb.DeleteTableOutput, error)
}

func NewDynamoDBClient(tableName string, keySchemaInput KeySchemaInput, gsiKeySchemaInput []*GsiKeySchemaInput) (*DynamoDBClient, error) {
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}
	if keySchemaInput.HashKey == "" {
		return nil, errors.New("hash key in key schema cannot be empty")
	}
	for _, gsi := range gsiKeySchemaInput {
		if gsi.IndexName == "" {
			return nil, errors.New("GSI index name cannot be empty")
		}
		if gsi.HashKey == "" {
			return nil, errors.New("GSI hash key cannot be empty")
		}
		if gsi.ProjectionType != "" {
			if gsi.ProjectionType != "ALL" && gsi.ProjectionType != "INCLUDE" && gsi.ProjectionType != "KEYS_ONLY" {
				return nil, errors.New("GSI projection type must be one of ALL, INCLUDE, or KEYS_ONLY")
			}
		}
	}

	initAwsDynamoDb()
	return &DynamoDBClient{
		tableName:    tableName,
		keySchema:    keySchemaInput,
		gsiKeySchema: gsiKeySchemaInput,
	}, nil
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
