package models

import "time"

// Now returns current UTC time; extracted for testability.
func Now() time.Time {
	return time.Now().UTC()
}
