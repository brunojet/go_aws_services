package dynamodb

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func convertKeySchema(input KeySchemaInput, attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool) []*dynamodb.KeySchemaElement {
	if input.HashKey == "" {
		panic(errors.New("HASH key is required"))
	}

	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(input.HashKey),
			KeyType:       aws.String("HASH"),
		},
	}

	addAttributeDefinition(attributeDefinitions, attributeMap, input.HashKey, "S")

	if input.RangeKey != "" {
		keySchema = append(keySchema, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(input.RangeKey),
			KeyType:       aws.String("RANGE"),
		})

		attributeType := "S"
		if input.RangeType != "" {
			attributeType = input.RangeType
		}

		addAttributeDefinition(attributeDefinitions, attributeMap, input.RangeKey, attributeType)
	}

	return keySchema
}

func convertGSI(inputs []*GsiKeySchemaInput, attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool) []*dynamodb.GlobalSecondaryIndex {
	var gsis []*dynamodb.GlobalSecondaryIndex

	for _, input := range inputs {
		keySchema := convertKeySchema(input.KeySchemaInput, attributeDefinitions, attributeMap)

		projection := &dynamodb.Projection{
			ProjectionType: aws.String(input.ProjectionType),
		}

		if len(input.NonKeyAttributes) > 0 {
			projection.NonKeyAttributes = aws.StringSlice(input.NonKeyAttributes)
		}

		gsi := &dynamodb.GlobalSecondaryIndex{
			IndexName:  aws.String(input.IndexName),
			KeySchema:  keySchema,
			Projection: projection,
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(input.ReadCapacityUnits),
				WriteCapacityUnits: aws.Int64(input.WriteCapacityUnits),
			},
		}

		gsis = append(gsis, gsi)
	}

	return gsis
}

func addAttributeDefinition(attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool, attributeName, attributeType string) {
	if attributeMap[attributeName] {
		return
	}
	attributeMap[attributeName] = true
	*attributeDefinitions = append(*attributeDefinitions, &dynamodb.AttributeDefinition{
		AttributeName: aws.String(attributeName),
		AttributeType: aws.String(attributeType),
	})
}

func findGsiKeySchema(gsiKeySchema []*GsiKeySchemaInput, indexName string) *GsiKeySchemaInput {
	for _, gsi := range gsiKeySchema {
		if gsi.IndexName == indexName {
			return gsi
		}
	}
	panic(errors.New("GSI index not found"))
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
