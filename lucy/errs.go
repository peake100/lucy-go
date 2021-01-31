package lucy

import (
	"github.com/peake100/gRPEAKEC-go/pkerr"
	"google.golang.org/grpc/codes"
)

// Sentinels is the pkerr.SentinelIssuer for lucy, and creates sentinel errors with
// an issuer of "Lucy".
var Sentinels = pkerr.NewSentinelIssuer("Lucy", true)

// ErrWrongJobStageStatus is returned when a job stage update cannot be made because the
// stage status is in the wrong state.
//
// This error will be returned, for instance, if a completed update is sent before
// the stage has been started.
var ErrWrongJobStageStatus = Sentinels.NewSentinel(
	"StageState",
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