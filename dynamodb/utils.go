package dynamodb

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func generateKeySchema(key string, keyType string) []*dynamodb.KeySchemaElement {
	if key == "" {
		panic(errors.New("key is required"))
	}

	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(key),
			KeyType:       aws.String(keyType),
		},
	}
	return keySchema
}

func convertKeySchema(input KeySchemaInput, attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool) []*dynamodb.KeySchemaElement {
	keySchema := generateKeySchema(input.HashKey, HashKeyType)

	addAttributeDefinition(attributeDefinitions, attributeMap, input.HashKey, AttrValString)

	if input.RangeKey != "" {
		rangeSchema := generateKeySchema(input.RangeKey, RangeKeyType)
		keySchema = append(keySchema, rangeSchema...)

		addAttributeDefinition(attributeDefinitions, attributeMap, input.RangeKey, input.RangeType)
	}

	return keySchema
}

func addProjection(gsi *dynamodb.GlobalSecondaryIndex, projectionType string, nonKeyAttributes []string) {
	projection := &dynamodb.Projection{
		ProjectionType: aws.String(projectionType),
	}

	if projectionType == ProjectionTypeInclude {
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
		attributeType = AttrValString
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

func addConditionExpression(expressionAttributeNames map[string]*string, expressionAttributeValues map[string]*dynamodb.AttributeValue, conditionExpression *string, key string, value string) {
	expressionAttributeNames["#"+key] = aws.String(key)
	expressionAttributeValues[":"+key] = &dynamodb.AttributeValue{
		S: aws.String(value),
	}

	if *conditionExpression != "" {
		*conditionExpression += " AND "
	}
	*conditionExpression += "#" + key + " = :" + key
}

func buildKeyConditionExpression(gsiKeySchema *GsiKeySchemaInput, key map[string]interface{}) (string, map[string]*string, map[string]*dynamodb.AttributeValue) {
	expressionAttributeNames := make(map[string]*string)
	expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)
	keyConditionExpression := ""

	for k, v := range key {
		switch k {
		case gsiKeySchema.HashKey:
			fallthrough
		case gsiKeySchema.RangeKey:
			addConditionExpression(expressionAttributeNames, expressionAttributeValues, &keyConditionExpression, k, v.(string))
		}
	}

	return keyConditionExpression, expressionAttributeNames, expressionAttributeValues
}
