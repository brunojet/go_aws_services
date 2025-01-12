package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateSchemaIntegrity(t *testing.T) {
	t.Run("Valid Hash Key", func(t *testing.T) {
		input := KeySchemaInput{HashKey: "id"}
		err := validateSchemaIntegrity(input)
		assert.NoError(t, err)
	})

	t.Run("Empty Hash Key", func(t *testing.T) {
		input := KeySchemaInput{HashKey: ""}
		err := validateSchemaIntegrity(input)
		assert.Error(t, err)
		assert.Equal(t, "hash key cannot be empty", err.Error())
	})
}

func TestValidateNonKeyAttributes(t *testing.T) {
	t.Run("Valid Non-Key Attributes", func(t *testing.T) {
		nonKeyAttributes := []string{"attribute1"}
		err := validateNonKeyAttributes(nonKeyAttributes)
		assert.NoError(t, err)
	})

	t.Run("Empty Non-Key Attributes", func(t *testing.T) {
		nonKeyAttributes := []string{}
		err := validateNonKeyAttributes(nonKeyAttributes)
		assert.Error(t, err)
		assert.Equal(t, "GSI projection type INCLUDE must have at least one non-key attribute", err.Error())
	})
}

func TestValidateGsiSchemaProjections(t *testing.T) {
	t.Run("Valid Projection Type ALL", func(t *testing.T) {
		input := &GsiKeySchemaInput{ProjectionType: "ALL"}
		err := validateGsiSchemaProjections(input)
		assert.NoError(t, err)
	})

	t.Run("Valid Projection Type KEYS_ONLY", func(t *testing.T) {
		input := &GsiKeySchemaInput{ProjectionType: "KEYS_ONLY"}
		err := validateGsiSchemaProjections(input)
		assert.NoError(t, err)
	})

	t.Run("Valid Projection Type INCLUDE with Non-Key Attributes", func(t *testing.T) {
		input := &GsiKeySchemaInput{ProjectionType: "INCLUDE", NonKeyAttributes: []string{"attribute1"}}
		err := validateGsiSchemaProjections(input)
		assert.NoError(t, err)
	})

	t.Run("Valid Projection Type INCLUDE with Empty Non-Key Attributes", func(t *testing.T) {
		input := &GsiKeySchemaInput{ProjectionType: "INCLUDE", NonKeyAttributes: []string{}}
		err := validateGsiSchemaProjections(input)
		assert.Error(t, err)
		assert.Equal(t, "GSI projection type INCLUDE must have at least one non-key attribute", err.Error())
	})

	t.Run("Invalid Projection Type", func(t *testing.T) {
		input := &GsiKeySchemaInput{ProjectionType: "INVALID"}
		err := validateGsiSchemaProjections(input)
		assert.Error(t, err)
		assert.Equal(t, "GSI projection type must be one of ALL, INCLUDE, or KEYS_ONLY", err.Error())
	})
}

func TestValidateGsiSchemaIntegrity(t *testing.T) {
	t.Run("Valid GSI Schema", func(t *testing.T) {
		input := []*GsiKeySchemaInput{
			{
				KeySchemaInput: KeySchemaInput{HashKey: "id"},
				IndexName:      "index1",
				ProjectionType: "ALL",
			},
		}
		err := validateGsiSchemaIntegrity(input)
		assert.NoError(t, err)
	})

	t.Run("Empty Hash Key", func(t *testing.T) {
		input := []*GsiKeySchemaInput{
			{
				KeySchemaInput: KeySchemaInput{HashKey: ""},
				IndexName:      "index1",
				ProjectionType: "ALL",
			},
		}
		err := validateGsiSchemaIntegrity(input)
		assert.Error(t, err)
		assert.Equal(t, "hash key cannot be empty", err.Error())
	})

	t.Run("Empty Index Name", func(t *testing.T) {
		input := []*GsiKeySchemaInput{
			{
				KeySchemaInput: KeySchemaInput{HashKey: "id"},
				IndexName:      "",
				ProjectionType: "ALL",
			},
		}
		err := validateGsiSchemaIntegrity(input)
		assert.Error(t, err)
		assert.Equal(t, "GSI index name cannot be empty", err.Error())
	})

	t.Run("Invalid Projection Type", func(t *testing.T) {
		input := []*GsiKeySchemaInput{
			{
				KeySchemaInput: KeySchemaInput{HashKey: "id"},
				IndexName:      "index1",
				ProjectionType: "INVALID",
			},
		}
		err := validateGsiSchemaIntegrity(input)
		assert.Error(t, err)
		assert.Equal(t, "GSI projection type must be one of ALL, INCLUDE, or KEYS_ONLY", err.Error())
	})
}
