package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

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
	return d.client.PutItem(input)
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
	gsiKeySchema, err := findGsiKeySchema(d.gsiKeySchema, indexName)
	if err != nil {
		return nil, err
	}

	keyConditionExpression, expressionAttributeNames, expressionAttributeValues := buildKeyConditionExpression(gsiKeySchema, key)

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(d.tableName),
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	return d.client.Query(input)
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
	return d.client.GetItem(input)
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
	return d.client.DeleteItem(input)
}
