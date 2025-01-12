package dynamodb

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var _ DynamoDBService = (*DynamoDBClient)(nil)

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
		if gsi.ProjectionType != "ALL" && gsi.ProjectionType != "INCLUDE" && gsi.ProjectionType != "KEYS_ONLY" {
			return nil, errors.New("GSI projection type must be one of ALL, INCLUDE, or KEYS_ONLY")
		}
	}

	initAwsDynamoDb()
	return &DynamoDBClient{
		tableName:    tableName,
		keySchema:    keySchemaInput,
		gsiKeySchema: gsiKeySchemaInput,
	}, nil
}

func (d *DynamoDBClient) CreateTableAsync() (*dynamodb.CreateTableOutput, error) {
	attributeDefinitions := []*dynamodb.AttributeDefinition{}
	attributeMap := make(map[string]bool)

	keySchema := convertKeySchema(d.keySchema, &attributeDefinitions, attributeMap)

	var globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex
	if d.gsiKeySchema != nil {
		globalSecondaryIndexes = convertGSI(d.gsiKeySchema, &attributeDefinitions, attributeMap)
	}

	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(d.tableName),
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(d.keySchema.ReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(d.keySchema.WriteCapacityUnits),
		},
	}

	if len(globalSecondaryIndexes) > 0 {
		input.GlobalSecondaryIndexes = globalSecondaryIndexes
	}

	return dynamoClient.CreateTable(input)
}

func (d *DynamoDBClient) CreateTable() (*dynamodb.CreateTableOutput, error) {
	output, err := d.CreateTableAsync()
	if err != nil {
		return nil, err
	}

	err = dynamoClient.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	})

	if err != nil {
		return nil, err
	}

	return output, nil
}

func (d *DynamoDBClient) DeleteTableAsync() (*dynamodb.DeleteTableOutput, error) {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(d.tableName),
	}

	return dynamoClient.DeleteTable(input)
}

func (d *DynamoDBClient) DeleteTable() (*dynamodb.DeleteTableOutput, error) {
	output, err := d.DeleteTableAsync()
	if err != nil {
		return nil, err
	}

	err = dynamoClient.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	})

	if err != nil {
		return nil, err
	}

	return output, nil
}

// PutItem inserts an item into the DynamoDB table.
// It takes an item as input, marshals it into a DynamoDB attribute value map,
// and then calls the PutItem method of the DynamoDB client.
//
// Parameters:
//
//	item (map[string]interface{}): The item to insert.
//
// Returns:
//
//	(*dynamodb.PutItemOutput, error): The output from the PutItem operation, or an error if the operation failed.
func (d *DynamoDBClient) PutItem(item map[string]interface{}) (*dynamodb.PutItemOutput, error) {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      av,
	}
	return dynamoClient.PutItem(input)
}

// QueryItem queries items from the DynamoDB table using a secondary index.
// It takes a key and an index name as input, builds the key condition expression,
// and then calls the Query method of the DynamoDB client.
//
// Parameters:
//
//	key (map[string]interface{}): The key to query.
//	indexName (string): The name of the secondary index.
//
// Returns:
//
//	(*dynamodb.QueryOutput, error): The output from the Query operation, or an error if the operation failed.
func (d *DynamoDBClient) QueryItem(key map[string]interface{}, indexName string) (*dynamodb.QueryOutput, error) {
	gsiKeySchema := findGsiKeySchema(d.gsiKeySchema, indexName)

	keyConditionExpression, expressionAttributeNames, expressionAttributeValues := buildKeyConditionExpression(gsiKeySchema, key)

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(d.tableName),
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	return dynamoClient.Query(input)
}

// GetItem retrieves an item from the DynamoDB table.
// It takes a key as input, marshals it into a DynamoDB attribute value map,
// and then calls the GetItem method of the DynamoDB client.
//
// Parameters:
//
//	key (map[string]interface{}): The key of the item to retrieve.
//
// Returns:
//
//	(*dynamodb.GetItemOutput, error): The output from the GetItem operation, or an error if the operation failed.
func (d *DynamoDBClient) GetItem(key map[string]interface{}) (*dynamodb.GetItemOutput, error) {
	av, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key:       av,
	}
	return dynamoClient.GetItem(input)
}

// DeleteItem deletes an item from the DynamoDB table.
// It takes a key as input, marshals it into a DynamoDB attribute value map,
// and then calls the DeleteItem method of the DynamoDB client.
//
// Parameters:
//
//	key (map[string]interface{}): The key of the item to delete.
//
// Returns:
//
//	(*dynamodb.DeleteItemOutput, error): The output from the DeleteItem operation, or an error if the operation failed.
func (d *DynamoDBClient) DeleteItem(key map[string]interface{}) (*dynamodb.DeleteItemOutput, error) {
	av, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(d.tableName),
		Key:       av,
	}
	return dynamoClient.DeleteItem(input)
}
