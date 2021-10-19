package pipeline

const (
	errMissingStartDate = "missing mandatory parameter: start"
	errMissingEndDate   = "missing mandatory parameter: end"
	errInvalidDateRange = "the end date must be after the start date"
	errMissingStep      = "missing mandatory parameter: step"
	errAlreadyRunning   = "the pipeline is running already"
)
