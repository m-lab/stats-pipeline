package pipeline

const (
	errMissingStartDate = "missing mandatory parameter: start"
	errMissingEndDate   = "missing mandatory parameter: end"
	errInvalidDateRange = "the start and end date must be in the same year (with start < end)"
	errMissingStep      = "missing mandatory parameter: step"
	errAlreadyRunning   = "the pipeline is running already"
)
