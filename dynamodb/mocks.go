package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
	mock.Mock
}

func (m *mockDynamoDBClient) New(p client.ConfigProvider) *dynamodb.DynamoDB {
	args := m.Called(p)
	return args.Get(0).(*dynamodb.DynamoDB)
}

func (m *mockDynamoDBClient) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*dynamodb.CreateTableOutput), args.Error(1)
}

func (m *mockDynamoDBClient) WaitUntilTableExists(input *dynamodb.DescribeTableInput) error {
	args := m.Called(input)
	return args.Error(0)
}

func (m *mockDynamoDBClient) DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*dynamodb.DeleteTableOutput), args.Error(1)
}

func (m *mockDynamoDBClient) WaitUntilTableNotExists(input *dynamodb.DescribeTableInput) error {
	args := m.Called(input)
	return args.Error(0)
}

func (m *mockDynamoDBClient) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*dynamodb.PutItemOutput), args.Error(1)
}

func (m *mockDynamoDBClient) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*dynamodb.QueryOutput), args.Error(1)
}

func (m *mockDynamoDBClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*dynamodb.GetItemOutput), args.Error(1)
}

func (m *mockDynamoDBClient) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*dynamodb.DeleteItemOutput), args.Error(1)
}

func mockNewDynamoDBClient(tableName string, keySchemaInput KeySchemaInput, gsiKeySchemaInput []*GsiKeySchemaInput) (*DynamoDBClient, *mockDynamoDBClient, error) {
	mockClient := new(mockDynamoDBClient)

	dynamoClient = mockClient
	isInitiated = true

	dynamoDbClient, err := NewDynamoDBClient(tableName, keySchemaInput, gsiKeySchemaInput)

	return dynamoDbClient, mockClient, err
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
