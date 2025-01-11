package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var keySchemaInput = KeySchemaInput{HashKey: "id", RangeKey: "range", ReadCapacityUnits: 1, WriteCapacityUnits: 1}

var gsiKeySchemaInput = []*GsiKeySchemaInput{
	{
		KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
		IndexName:      "GSI1",
		ProjectionType: "ALL",
	},
}

func TestCreateTableAsync(t *testing.T) {
	dynamoClient := NewDynamoDBClient("test-table", keySchemaInput, gsiKeySchemaInput)

	// Chamar a função CreateTableAsync
	result, err := dynamoClient.CreateTable()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-table", *result.TableDescription.TableName)

	// Limpar: Excluir a tabela após o teste
	_, err = dynamoClient.DeleteTable()
	assert.NoError(t, err)
}
