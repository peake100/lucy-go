package internal

import (
	"fmt"
	"github.com/illuscio-dev/protoCereal-go/cereal"
)

// NewRecordId creates a uuid and creation time for inserting new records.
func NewRecordId() (recordId *cereal.UUID, err error) {
	recordId, err = cereal.NewUUIDRandom()
	if err != nil {
		return nil, fmt.Errorf(
			"error generating new UUID: %w", err,
		)
	}

	return recordId, nil
}
