package dynamodb

import "github.com/aws/aws-sdk-go/service/dynamodb"

type DynamoDBClient struct {
	tableName    string
	keySchema    KeySchemaInput
	gsiKeySchema []*GsiKeySchemaInput
	client       *dynamodb.DynamoDB
}

type KeySchemaInput struct {
	HashKey            string `json:"HASH"`
	RangeKey           string `json:"RANGE,omitempty"`
	RangeType          string `json:"RANGE_TYPE,omitempty"`
	ReadCapacityUnits  int64  `json:"readCapacityUnits"`
	WriteCapacityUnits int64  `json:"writeCapacityUnits"`
}

type GsiKeySchemaInput struct {
	KeySchemaInput
	IndexName        string   `json:"IndexName"`
	ProjectionType   string   `json:"ProjectionType"`
	NonKeyAttributes []string `json:"NonKeyAttributes,omitempty"`
}
