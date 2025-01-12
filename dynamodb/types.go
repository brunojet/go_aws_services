package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DynamoDBClient struct {
	tableName    string
	keySchema    KeySchemaInput
	gsiKeySchema []*GsiKeySchemaInput
	client       dynamodbiface.DynamoDBAPI
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

const (
	HashKeyType            = "HASH"
	RangeKeyType           = "RANGE"
	AttrValString          = "S"
	AttrValInteger         = "N"
	ProjectionTypeAll      = "ALL"
	ProjectionTypeKeysOnly = "KEYS_ONLY"
	ProjectionTypeInclude  = "INCLUDE"
)

type DynamoDBService interface {
	PutItem(item map[string]interface{}) (*dynamodb.PutItemOutput, error)
	QueryItem(key map[string]interface{}, indexName string) (*dynamodb.QueryOutput, error)
	GetItem(key map[string]interface{}) (*dynamodb.GetItemOutput, error)
	CreateTableAsync() (*dynamodb.CreateTableOutput, error)
	CreateTable() (*dynamodb.CreateTableOutput, error)
	DeleteTableAsync() (*dynamodb.DeleteTableOutput, error)
	DeleteTable() (*dynamodb.DeleteTableOutput, error)
}
