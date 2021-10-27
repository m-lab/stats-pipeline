package pipeline

import "errors"

var (
	errMissingStartDate = errors.New("missing mandatory parameter: start")
	errMissingEndDate   = errors.New("missing mandatory parameter: end")
	errInvalidDateRange = errors.New("the end date must be after the start date")
	errMissingStep      = errors.New("missing mandatory parameter: step")
	errAlreadyRunning   = errors.New("the pipeline is running already")
)
