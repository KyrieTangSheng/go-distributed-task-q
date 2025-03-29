package broker

import "fmt"

// Error represents a broker-related error
type Error struct {
	Message string
}

// NewError creates a new Error with the given message
func NewError(message string) *Error {
	return &Error{
		Message: message,
	}
}

// Error implements the error interface
func (e *Error) Error() string {
	return e.Message
}

// Is implements error comparison for errors.Is
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Message == t.Message
}

// WithDetails adds context details to an error
func (e *Error) WithDetails(format string, args ...interface{}) *Error {
	return &Error{
		Message: fmt.Sprintf("%s: %s", e.Message, fmt.Sprintf(format, args...)),
	}
}
