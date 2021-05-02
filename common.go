package kafka

import (
	"context"
)

// BeforeFunc takes information from context, treat it and put it again in the context.
// This function will be executed before DecodeRequestFunc
type BeforeFunc func(context.Context, interface{}) context.Context

// AfterFunc takes information from context, treat it and put it again in the context.
// This function will be executed after Endpoint but before EncodeResponseFunc
type AfterFunc func(context.Context, interface{}) context.Context

// FinalizerFunc is executed on quitting the function. It returns nothing.
type FinalizerFunc func(context.Context, interface{})
