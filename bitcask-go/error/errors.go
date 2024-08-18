package error

import "errors"

var (
	EmptyKeyError         = errors.New("key is empty")
	IndexFailError        = errors.New("index is failed")
	KeyNotExistsError     = errors.New("key not exists")
	DataFileNotFoundError = errors.New("data file not exists")
	DataDirCorruptedError = errors.New("data directory is corrupted")

	ExceedMaxBatchSizeError = errors.New("max batch size exceed")
	MergeInProgressError    = errors.New("merge in progress")
	DataBaseIsUsingError    = errors.New("data base is using error")
)
