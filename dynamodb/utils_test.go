package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestConvertKeySchema(t *testing.T) {
	t.Run("Valid key schema with hash and range", func(t *testing.T) {
		input := KeySchemaInput{
			HashKey:  "id",
			RangeKey: "range",
		}
		var attributeDefinitions []*dynamodb.AttributeDefinition
		attributeMap := make(map[string]bool)

		keySchema := convertKeySchema(input, &attributeDefinitions, attributeMap)

		assert.Equal(t, 2, len(keySchema))
		assert.Equal(t, "id", *keySchema[0].AttributeName)
		assert.Equal(t, HashKeyType, *keySchema[0].KeyType)
		assert.Equal(t, "range", *keySchema[1].AttributeName)
		assert.Equal(t, RangeKeyType, *keySchema[1].KeyType)

		assert.Equal(t, 2, len(attributeDefinitions))
		assert.Equal(t, "id", *attributeDefinitions[0].AttributeName)
		assert.Equal(t, AttrValString, *attributeDefinitions[0].AttributeType)
		assert.Equal(t, "range", *attributeDefinitions[1].AttributeName)
		assert.Equal(t, AttrValString, *attributeDefinitions[1].AttributeType)
	})

	t.Run("Valid key schema with only hash", func(t *testing.T) {
		input := KeySchemaInput{
			HashKey: "id",
		}
		var attributeDefinitions []*dynamodb.AttributeDefinition
		attributeMap := make(map[string]bool)

		keySchema := convertKeySchema(input, &attributeDefinitions, attributeMap)

		assert.Equal(t, 1, len(keySchema))
		assert.Equal(t, "id", *keySchema[0].AttributeName)
		assert.Equal(t, HashKeyType, *keySchema[0].KeyType)

		assert.Equal(t, 1, len(attributeDefinitions))
		assert.Equal(t, "id", *attributeDefinitions[0].AttributeName)
		assert.Equal(t, AttrValString, *attributeDefinitions[0].AttributeType)
	})

	t.Run("Panic on missing hash key", func(t *testing.T) {
		input := KeySchemaInput{}
		var attributeDefinitions []*dynamodb.AttributeDefinition
		attributeMap := make(map[string]bool)

		assert.PanicsWithError(t, "key is required", func() {
			convertKeySchema(input, &attributeDefinitions, attributeMap)
		})
	})
}

func TestConvertGSI(t *testing.T) {
	t.Run("Valid GSI conversion", func(t *testing.T) {
		inputs := []*GsiKeySchemaInput{
			{
				KeySchemaInput: KeySchemaInput{
					HashKey:            "field1",
					RangeKey:           "field2",
					ReadCapacityUnits:  1,
					WriteCapacityUnits: 1,
				},
				IndexName:      "GSI1",
				ProjectionType: "ALL",
			},
		}
		var attributeDefinitions []*dynamodb.AttributeDefinition
		attributeMap := make(map[string]bool)

		gsis := convertGSI(inputs, &attributeDefinitions, attributeMap)

		assert.Equal(t, 1, len(gsis))
		assert.Equal(t, "GSI1", *gsis[0].IndexName)
		assert.Equal(t, "ALL", *gsis[0].Projection.ProjectionType)
		assert.Equal(t, int64(1), *gsis[0].ProvisionedThroughput.ReadCapacityUnits)
		assert.Equal(t, int64(1), *gsis[0].ProvisionedThroughput.WriteCapacityUnits)

		assert.Equal(t, 2, len(attributeDefinitions))
		assert.Equal(t, "field1", *attributeDefinitions[0].AttributeName)
		assert.Equal(t, AttrValString, *attributeDefinitions[0].AttributeType)
		assert.Equal(t, "field2", *attributeDefinitions[1].AttributeName)
		assert.Equal(t, AttrValString, *attributeDefinitions[1].AttributeType)
	})
}

func TestAddAttributeDefinition(t *testing.T) {
	t.Run("Add new attribute definition", func(t *testing.T) {
		var attributeDefinitions []*dynamodb.AttributeDefinition
		attributeMap := make(map[string]bool)

		addAttributeDefinition(&attributeDefinitions, attributeMap, "id", AttrValString)

		assert.Equal(t, 1, len(attributeDefinitions))
		assert.Equal(t, "id", *attributeDefinitions[0].AttributeName)
		assert.Equal(t, AttrValString, *attributeDefinitions[0].AttributeType)
		assert.True(t, attributeMap["id"])
	})

	t.Run("Do not add duplicate attribute definition", func(t *testing.T) {
		var attributeDefinitions []*dynamodb.AttributeDefinition
		attributeMap := make(map[string]bool)
		attributeMap["id"] = true

		addAttributeDefinition(&attributeDefinitions, attributeMap, "id", AttrValString)

		assert.Equal(t, 0, len(attributeDefinitions))
	})
}

func TestFindGsiKeySchema(t *testing.T) {
	gsiKeySchemaInput := []*GsiKeySchemaInput{
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field1", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI1",
			ProjectionType: "ALL",
		},
		{
			KeySchemaInput: KeySchemaInput{HashKey: "field2", ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			IndexName:      "GSI2",
			ProjectionType: "ALL",
		},
	}

	t.Run("GSI key schema found", func(t *testing.T) {
		gsi := findGsiKeySchema(gsiKeySchemaInput, "GSI1")
		assert.NotNil(t, gsi)
		assert.Equal(t, "GSI1", gsi.IndexName)
	})

	t.Run("GSI key schema not found", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				assert.Equal(t, "GSI index not found", r.(error).Error())
			}
		}()
		gsi := findGsiKeySchema(gsiKeySchemaInput, "GSI3")
		assert.Nil(t, gsi)
	})
}

func TestBuildKeyConditionExpression(t *testing.T) {
	gsiKeySchema := &GsiKeySchemaInput{
		KeySchemaInput: KeySchemaInput{
			HashKey:  "id",
			RangeKey: "range",
		},
	}

	t.Run("Valid key with hash and range", func(t *testing.T) {
		key := map[string]interface{}{
			"id":    "123",
			"range": "456",
		}
		expression, names, values := buildKeyConditionExpression(gsiKeySchema, key)
		assert.Contains(t, expression, "#id = :id")
		assert.Contains(t, expression, "#range = :range")
		assert.Contains(t, expression, " AND ")
		assert.Equal(t, map[string]*string{
			"#id":    aws.String("id"),
			"#range": aws.String("range"),
		}, names)
		assert.Equal(t, map[string]*dynamodb.AttributeValue{
			":id":    {S: aws.String("123")},
			":range": {S: aws.String("456")},
		}, values)
	})

	t.Run("Valid key with only hash", func(t *testing.T) {
		gsiKeySchema.RangeKey = ""
		key := map[string]interface{}{
			"id": "123",
		}
		expression, names, values := buildKeyConditionExpression(gsiKeySchema, key)
		assert.Equal(t, "#id = :id", expression)
		assert.Equal(t, map[string]*string{
			"#id": aws.String("id"),
		}, names)
		assert.Equal(t, map[string]*dynamodb.AttributeValue{
			":id": {S: aws.String("123")},
		}, values)
	})

	t.Run("Valid key with only range", func(t *testing.T) {
		gsiKeySchema.HashKey = ""
		gsiKeySchema.RangeKey = "range"
		key := map[string]interface{}{
			"range": "456",
		}
		expression, names, values := buildKeyConditionExpression(gsiKeySchema, key)
		assert.Equal(t, "#range = :range", expression)
		assert.Equal(t, map[string]*string{
			"#range": aws.String("range"),
		}, names)
		assert.Equal(t, map[string]*dynamodb.AttributeValue{
			":range": {S: aws.String("456")},
		}, values)
	})
}
