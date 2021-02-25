package service

/*
This file contains all lucy rpc methods for fetching jobs / batches.
*/

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/illuscio-dev/protoCereal-go/cereal"
	"github.com/peake100/lucy-go/pkg/lucy"
	"io"
)

// GetBatch implements lucy.LucyServer.
func (service Lucy) GetBatch(
	ctx context.Context, uuid *cereal.UUID,
) (batch *lucy.Batch, err error) {
	result, err := service.db.GetBatch(ctx, uuid)
	return result.Batch, nil
}

// GetJob implements lucy.LucyServer.
func (service Lucy) GetJob(
	ctx context.Context, uuid *cereal.UUID,
) (*lucy.Job, error) {
	result, err := service.db.GetJob(ctx, uuid)
	return result.Job, err
}

// GetBatchJobs implements lucy.LucyServer.
func (service Lucy) GetBatchJobs(
	ctx context.Context, uuid *cereal.UUID,
) (*lucy.BatchJobs, error) {
	result, err := service.db.GetBatchJobs(ctx, uuid)

	return result.Jobs, err
}

// ListBatches implements lucy.LucyServer.
func (service Lucy) ListBatches(
	_ *empty.Empty, server lucy.Lucy_ListBatchesServer,
) error {
	// Get a batch cursor from our backend.
	cursor, err := service.db.ListBatches(server.Context())
	if err != nil {
		return fmt.Errorf("error getting batch cursor: %w", err)
	}

	// Exhaust the cursor.
	for {
		// Advance the cursor
		batch, err := cursor.Next(server.Context())

		// If the error is an io.EOF error, return cleanly.
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			// Otherwise if the error is non-nil, exit with the error.
			return fmt.Errorf("error fetching batch: %w", err)
		}

		// Send this batch back to the user.
		err = server.Send(batch)
		if err != nil {
			return fmt.Errorf("error sending batch: %w", err)
		}
	}
}
