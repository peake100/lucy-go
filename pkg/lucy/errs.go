package lucy

import (
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"google.golang.org/grpc/codes"
)

// Sentinels is the pkerr.SentinelIssuer for lucy, and creates sentinel errors with
// an issuer of "Jobs".
var Sentinels = pkerr.NewSentinelIssuer("Jobs", true)

// ErrInvalidStageStatus is returned when a job stage update cannot be made because the
// stage status is in the wrong state.
//
// This error will be returned, for instance, if a completed update is sent before
// the stage has been started.
var ErrInvalidStageStatus = Sentinels.NewSentinel(
	"InvalidStageStatus",
	2000,
	codes.FailedPrecondition,
	"job stage was not in correct state for update",
)

// ErrJobCancelled is returned when a job stage cannot be updated because the job was
// cancelled.
var ErrJobCancelled = Sentinels.NewSentinel(
	"JobCancelled",
	2001,
	codes.FailedPrecondition,
	"job is cancelled",
)

// ErrWorkerRestricted is returned when a job restricted to a certain worker receives
// a start, update, or complete command from another worker.
var ErrMaxRetriesExceeded = Sentinels.NewSentinel(
	"MaxRetriesExceeded",
	2002,
	codes.FailedPrecondition,
	"starting job would exceed max retry limit",
)

// ErrWorkerRestricted is returned when a job restricted to a certain worker receives
// a start, update, or complete command from another worker.
var ErrWorkerRestricted = Sentinels.NewSentinel(
	"WorkerRestricted",
	2003,
	codes.PermissionDenied,
	"job cannot be run by requesting worker",
)
