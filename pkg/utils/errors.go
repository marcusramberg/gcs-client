package utils

import "errors"

var (
	ErrInvalidArgs        = errors.New("invalid arguments")
	ErrOnlyGCS            = errors.New("only gs:// paths are supported")
	ErrGenerationRequired = errors.New("generation number is required")
)
