package cache

import "errors"

var (
	CacheEmptyError      = errors.New("for cache empty")
	CacheHandlerNotFound = errors.New("not found cache handler registered")
)
