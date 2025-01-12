package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestInitAwsDynamoDb(t *testing.T) {
	mockSession := new(mockCustomSession)
	mockSession.On("GetAWSSession").Return(&session.Session{
		Config: &aws.Config{
			Region: aws.String("us-east-1"),
		},
	})

	mockDynamoDB := new(mockDynamoDB)

	oldAwsSession := getAwsSession
	oldNewdynamodb := newDynamodb

	getAwsSession = mockSession.GetAWSSession
	newDynamodb = mockDynamoDB.New

	defer func() {
		getAwsSession = oldAwsSession
		newDynamodb = oldNewdynamodb
	}()

	t.Run("Valid dynamodb", func(t *testing.T) {
		mockDynamoDB.On("New", mock.AnythingOfType("*session.Session"), mock.Anything).Return(&dynamodb.DynamoDB{}).Once()

		client := initAwsDynamoDb()

		assert.NotNil(t, client)
	})

	t.Run("Nil dynamodb", func(t *testing.T) {
		mockDynamoDB.On("New", mock.AnythingOfType("*session.Session"), mock.Anything).Return(nil).Once()

		dynamoClient = nil

		assert.Panics(t, func() {
			initAwsDynamoDb()
		})
	})

	mockSession.AssertExpectations(t)
	mockDynamoDB.AssertExpectations(t)
}
