package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DynamoDBClient) CreateTableAsync() (*dynamodb.CreateTableOutput, error) {
	attributeDefinitions := []*dynamodb.AttributeDefinition{}
	attributeMap := make(map[string]bool)

	keySchema, err := convertKeySchema(d.keySchema, &attributeDefinitions, attributeMap)
	if err != nil {
		return nil, err
	}

	var globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex
	if d.gsiKeySchema != nil {
		globalSecondaryIndexes, err = convertGSI(d.gsiKeySchema, &attributeDefinitions, attributeMap)
		if err != nil {
			return nil, err
		}
	}

	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(d.tableName),
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(d.keySchema.ReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(d.keySchema.WriteCapacityUnits),
		},
	}

	if len(globalSecondaryIndexes) > 0 {
		input.GlobalSecondaryIndexes = globalSecondaryIndexes
	}

	return d.client.CreateTable(input)
}

func (d *DynamoDBClient) CreateTable() (*dynamodb.CreateTableOutput, error) {
	output, err := d.CreateTableAsync()
	if err != nil {
		return nil, err
	}

	err = d.client.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	})

	if err != nil {
		return nil, err
	}

	return output, nil
}

func (d *DynamoDBClient) DeleteTableAsync() (*dynamodb.DeleteTableOutput, error) {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(d.tableName),
	}

	return d.client.DeleteTable(input)
}

func (d *DynamoDBClient) DeleteTable() (*dynamodb.DeleteTableOutput, error) {
	output, err := d.DeleteTableAsync()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err = d.client.WaitUntilTableNotExistsWithContext(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	})

	if err != nil {
		return nil, err
	}

	return output, nil
}

func convertKeySchema(input KeySchemaInput, attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool) ([]*dynamodb.KeySchemaElement, error) {
	if input.HashKey == "" {
		return nil, errors.New("HASH key is required")
	}

	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(input.HashKey),
			KeyType:       aws.String("HASH"),
		},
	}

	addAttributeDefinition(attributeDefinitions, attributeMap, input.HashKey, "S") // Default to string type

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

	return keySchema, nil
}

func convertGSI(inputs []*GsiKeySchemaInput, attributeDefinitions *[]*dynamodb.AttributeDefinition, attributeMap map[string]bool) ([]*dynamodb.GlobalSecondaryIndex, error) {
	var gsis []*dynamodb.GlobalSecondaryIndex

	for _, input := range inputs {
		keySchema, err := convertKeySchema(input.KeySchemaInput, attributeDefinitions, attributeMap)
		if err != nil {
			return nil, err
		}

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

	return gsis, nil
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
