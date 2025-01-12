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

		addAttributeDefinition(attributeDefinitions, attributeMap, input.RangeKey, input.RangeType)
	}

	return keySchema
}

func addProjection(gsi *dynamodb.GlobalSecondaryIndex, projectionType string, nonKeyAttributes []string) {
	projection := &dynamodb.Projection{
		ProjectionType: aws.String(projectionType),
	}

	if projectionType == "INCLUDE" {
		projection.NonKeyAttributes = aws.StringSlice(nonKeyAttributes)
	}

	gsi.Projection = projection
}

func addProvidedThroughput(gsi *dynamodb.GlobalSecondaryIndex, readCapacityUnits, writeCapacityUnits int64) {
	gsi.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(readCapacityUnits),
		WriteCapacityUnits: aws.Int64(writeCapacityUnits),
	}
}

func createGSI(input *GsiKeySchemaInput, attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool) *dynamodb.GlobalSecondaryIndex {
	keySchema := convertKeySchema(input.KeySchemaInput, attributeDefinitions, attributeMap)

	gsi := &dynamodb.GlobalSecondaryIndex{
		IndexName: aws.String(input.IndexName),
		KeySchema: keySchema,
	}

	addProjection(gsi, input.ProjectionType, input.NonKeyAttributes)
	addProvidedThroughput(gsi, input.ReadCapacityUnits, input.WriteCapacityUnits)

	return gsi
}

func convertGSI(inputs []*GsiKeySchemaInput, attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool) []*dynamodb.GlobalSecondaryIndex {
	var gsis []*dynamodb.GlobalSecondaryIndex

	for _, input := range inputs {
		gsi := createGSI(input, attributeDefinitions, attributeMap)
		gsis = append(gsis, gsi)
	}

	return gsis
}

func addAttributeDefinition(attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool, attributeName string, attributeType string) {
	if attributeMap[attributeName] {
		return
	}
	attributeMap[attributeName] = true

	if attributeType == "" {
		attributeType = "S"
	}

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
