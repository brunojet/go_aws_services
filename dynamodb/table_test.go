package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func (m *mockDynamoDBClient) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	println("mockDynamoDBClient.CreateTable")
	args := m.Called(input)
	return args.Get(0).(*dynamodb.CreateTableOutput), args.Error(1)
}

func (m *mockDynamoDBClient) WaitUntilTableExists(input *dynamodb.DescribeTableInput) error {
	println("mockDynamoDBClient.WaitUntilTableExists")
	args := m.Called(input)
	return args.Error(0)
}

func (m *mockDynamoDBClient) DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	println("mockDynamoDBClient.DeleteTable")
	args := m.Called(input)
	return args.Get(0).(*dynamodb.DeleteTableOutput), args.Error(1)
}

func (m *mockDynamoDBClient) WaitUntilTableNotExists(input *dynamodb.DescribeTableInput) error {
	println("mockDynamoDBClient.WaitUntilTableNotExists")
	args := m.Called(input)
	return args.Error(0)
}

func mockCreateTable(mockClient *mockDynamoDBClient) {
	mockClient.On("CreateTable", mock.AnythingOfType("*dynamodb.CreateTableInput")).Return(&dynamodb.CreateTableOutput{}, nil)
	mockClient.On("WaitUntilTableExists", mock.AnythingOfType("*dynamodb.DescribeTableInput")).Return(nil)
}

func mockDeleteTable(mockClient *mockDynamoDBClient) {
	mockClient.On("DeleteTable", mock.AnythingOfType("*dynamodb.DeleteTableInput")).Return(&dynamodb.DeleteTableOutput{}, nil)
	mockClient.On("WaitUntilTableNotExists", mock.AnythingOfType("*dynamodb.DescribeTableInput")).Return(nil)
}

func mockTable(t *testing.T, tableName string, keySchemaInput KeySchemaInput, gsiKeySchemaInput []*GsiKeySchemaInput) {
	dynamoClient, mockClient, _ := mockNewDynamoDBClient(tableName, keySchemaInput, gsiKeySchemaInput)

	t.Run("Creating table", func(t *testing.T) {
		mockCreateTable(mockClient)

		output, err := dynamoClient.CreateTable()

		assert.NotNil(t, output)
		assert.NoError(t, err)

		mockClient.AssertExpectations(t)
	})

	t.Run("Deleting table", func(t *testing.T) {
		mockDeleteTable(mockClient)

		output, err := dynamoClient.DeleteTable()

		assert.NotNil(t, output)
		assert.NoError(t, err)

		mockClient.AssertExpectations(t)
	})

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
