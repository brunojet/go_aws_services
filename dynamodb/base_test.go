package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockSession struct {
	mock.Mock
}

func (m *mockSession) NewSession(cfgs ...*aws.Config) (*session.Session, error) {
	args := m.Called(cfgs)
	return args.Get(0).(*session.Session), args.Error(1)
}

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
	mock.Mock
}

func (m *mockDynamoDBClient) New(p client.ConfigProvider) *dynamodb.DynamoDB {
	args := m.Called(p)
	return args.Get(0).(*dynamodb.DynamoDB)
}

func TestNewDynamoDBClient(t *testing.T) {
	mockSess := new(mockSession)
	mockSess.On("NewSession", mock.AnythingOfType("[]*aws.Config")).Return(&session.Session{}, nil)

	mockClient := new(mockDynamoDBClient)
	mockClient.On("New", mock.AnythingOfType("*session.Session")).Return(&dynamodb.DynamoDB{})

	// Substituir a função NewSession pelo mock
	originalNewSession := newSession
	newSession = func() *session.Session {
		sess, _ := mockSess.NewSession(&aws.Config{
			Region: aws.String("us-east-1"),
		})
		return sess
	}
	defer func() { newSession = originalNewSession }()

	// Substituir a função dynamodb.New pelo mock
	originalNew := newDynamodb
	newDynamodb = func(p client.ConfigProvider, cfgs ...*aws.Config) *dynamodb.DynamoDB {
		return mockClient.New(p)
	}
	defer func() { newDynamodb = originalNew }()

	// Definir os parâmetros de entrada
	tableName := "test-table"
	keySchemaInput := KeySchemaInput{HashKey: "id", RangeKey: "range", ReadCapacityUnits: 1, WriteCapacityUnits: 1}
	gsiKeySchemaInput := []*GsiKeySchemaInput{
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI1",
			ProjectionType: "ALL",
		},
	}

	// Chamar a função NewDynamoDBClient
	dynamoClient := NewDynamoDBClient(tableName, keySchemaInput, gsiKeySchemaInput)

	// Verificar se o cliente DynamoDB foi criado corretamente
	assert.NotNil(t, dynamoClient)
	assert.Equal(t, tableName, dynamoClient.tableName)
	assert.Equal(t, keySchemaInput, dynamoClient.keySchema)
	assert.Equal(t, gsiKeySchemaInput, dynamoClient.gsiKeySchema)

	// Verificar se os mocks foram chamados
	mockSess.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}
