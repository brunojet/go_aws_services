package dynamodb

import "errors"

func validateSchemaIntegrity(keySchemaInput KeySchemaInput) error {
	if keySchemaInput.HashKey == "" {
		return errors.New("hash key cannot be empty")
	}

	return nil
}

func validateNonKeyAttributes(nonKeyAttributes []string) error {
	if len(nonKeyAttributes) == 0 {
		return errors.New("GSI projection type INCLUDE must have at least one non-key attribute")
	}
	return nil
}

func validateGsiSchemaProjections(gsiKeySchemaInput *GsiKeySchemaInput) error {
	switch gsiKeySchemaInput.ProjectionType {
	case ProjectionTypeAll, ProjectionTypeKeysOnly:
		return nil
	case ProjectionTypeInclude:
		return validateNonKeyAttributes(gsiKeySchemaInput.NonKeyAttributes)
	default:
		return errors.New("GSI projection type must be one of ALL, INCLUDE, or KEYS_ONLY")
	}
}

func validateGsiSchemaIntegrity(gsiKeySchemaInput []*GsiKeySchemaInput) error {
	for _, gsi := range gsiKeySchemaInput {
		if gsi.IndexName == "" {
			return errors.New("GSI index name cannot be empty")
		}
		if err := validateSchemaIntegrity(gsi.KeySchemaInput); err != nil {
			return err
		}
		if err := validateGsiSchemaProjections(gsi); err != nil {
			return err
		}
	}

	return nil
}
