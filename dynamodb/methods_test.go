package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewDynamoDBClient(t *testing.T) {
	tableName := "test-table"
	keySchemaInput := KeySchemaInput{HashKey: "id", RangeKey: "range", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	gsiKeySchemaInput := []*GsiKeySchemaInput{
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI1",
			ProjectionType: "ALL",
		},
	}

	dynamoClient, mockClient, err := mockNewDynamoDBClient(tableName, keySchemaInput, gsiKeySchemaInput)

	assert.NotNil(t, dynamoClient)
	assert.Nil(t, err)
	assert.Equal(t, tableName, dynamoClient.tableName)
	assert.Equal(t, keySchemaInput, dynamoClient.keySchema)
	assert.Equal(t, gsiKeySchemaInput, dynamoClient.gsiKeySchema)

	mockClient.AssertExpectations(t)
}

func TestTable(t *testing.T) {
	tableName := "test-table"
	keySchemaInput := KeySchemaInput{HashKey: "id", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	mockTable(t, tableName, keySchemaInput, nil)
}

func TestTableWithRangeKey(t *testing.T) {
	tableName := "test-table-range"
	keySchemaInput := KeySchemaInput{HashKey: "id", RangeKey: "range", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	mockTable(t, tableName, keySchemaInput, nil)
}

func TestTableWithRangeKeyTyped(t *testing.T) {
	tableName := "test-table-range-numeric"
	keySchemaInput := KeySchemaInput{HashKey: "id", RangeKey: "range", RangeType: "N", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	mockTable(t, tableName, keySchemaInput, nil)
}

func TestTableWithGsi(t *testing.T) {
	tableName := "test-table-gsi"
	keySchemaInput := KeySchemaInput{HashKey: "id", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	gsiKeySchemaInput := []*GsiKeySchemaInput{
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI1",
			ProjectionType: "ALL",
		},
	}

	mockTable(t, tableName, keySchemaInput, gsiKeySchemaInput)
}

func TestTableWithGsiAndRangeKey(t *testing.T) {
	tableName := "test-table-gsi-range"
	keySchemaInput := KeySchemaInput{HashKey: "id", RangeKey: "range", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	gsiKeySchemaInput := []*GsiKeySchemaInput{
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI1",
			ProjectionType: "ALL",
		},
	}

	mockTable(t, tableName, keySchemaInput, gsiKeySchemaInput)
}

func TestTableWithGsiAndRangeKeyTyped(t *testing.T) {
	tableName := "test-table-gsi-range-numeric"
	keySchemaInput := KeySchemaInput{HashKey: "id", RangeKey: "range", RangeType: "N", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	gsiKeySchemaInput := []*GsiKeySchemaInput{
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI1",
			ProjectionType: "ALL",
		},
	}

	mockTable(t, tableName, keySchemaInput, gsiKeySchemaInput)
}

func TestPutItem(t *testing.T) {
	tableName := "test-table"
	keySchemaInput := KeySchemaInput{HashKey: "id", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	dynamoClient, mockClient, _ := mockNewDynamoDBClient(tableName, keySchemaInput, nil)

	item := map[string]interface{}{
		"id":   "123",
		"name": "test",
	}

	av, _ := dynamodbattribute.MarshalMap(item)
	expectedOutput := &dynamodb.PutItemOutput{}

	mockClient.On("PutItem", &dynamodb.PutItemInput{
		TableName: aws.String("test-table"),
		Item:      av,
	}).Return(expectedOutput, nil)

	output, err := dynamoClient.PutItem(item)

	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, output)
	mockClient.AssertExpectations(t)
}

func TestQueryItem(t *testing.T) {
	tableName := "test-table"
	keySchemaInput := KeySchemaInput{HashKey: "id", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	gsiKeySchemaInput := []*GsiKeySchemaInput{
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI1",
			ProjectionType: "ALL",
		},
	}
	dynamoClient, mockClient, _ := mockNewDynamoDBClient(tableName, keySchemaInput, gsiKeySchemaInput)

	key := map[string]interface{}{
		"field1": "value1",
	}

	expectedOutput := &dynamodb.QueryOutput{}
	mockClient.On("Query", mock.AnythingOfType("*dynamodb.QueryInput")).Return(expectedOutput, nil)

	output, err := dynamoClient.QueryItem(key, "GSI1")

	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, output)
	mockClient.AssertExpectations(t)
}

func TestGetItem(t *testing.T) {
	tableName := "test-table"
	keySchemaInput := KeySchemaInput{HashKey: "id", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	dynamoClient, mockClient, _ := mockNewDynamoDBClient(tableName, keySchemaInput, nil)

	key := map[string]interface{}{
		"id": "123",
	}

	av, _ := dynamodbattribute.MarshalMap(key)
	expectedOutput := &dynamodb.GetItemOutput{}

	mockClient.On("GetItem", &dynamodb.GetItemInput{
		TableName: aws.String("test-table"),
		Key:       av,
	}).Return(expectedOutput, nil)

	output, err := dynamoClient.GetItem(key)

	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, output)
	mockClient.AssertExpectations(t)
}

func TestDeleteItem(t *testing.T) {
	tableName := "test-table"
	keySchemaInput := KeySchemaInput{HashKey: "id", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	dynamoClient, mockClient, _ := mockNewDynamoDBClient(tableName, keySchemaInput, nil)

	key := map[string]interface{}{
		"id": "123",
	}

	av, _ := dynamodbattribute.MarshalMap(key)
	expectedOutput := &dynamodb.DeleteItemOutput{}

	mockClient.On("DeleteItem", &dynamodb.DeleteItemInput{
		TableName: aws.String("test-table"),
		Key:       av,
	}).Return(expectedOutput, nil)

	output, err := dynamoClient.DeleteItem(key)

	assert.NoError(t, err)
	assert.Equal(t, expectedOutput, output)
	mockClient.AssertExpectations(t)
}
