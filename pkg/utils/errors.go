package utils

import "errors"

var (
	ErrInvalidArgs        = errors.New("invalid arguments")
	ErrOnlyGCS            = errors.New("only gs:// paths are supported")
	ErrGenerationRequired = errors.New("generation number is required")
	ErrURLRequired        = errors.New("URL [URL ...]: Must be specified")
	ErrObjectRequired     = errors.New("URL must include an object name")
)
