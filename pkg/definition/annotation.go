package definition

import "fmt"

const (
	DeadlineDurationSuffix  = "deadline-duration"
	ExecutionDurationSuffix = "execution-duration"
)

// DeadlineDurationAnnotationKey returns the annotation key for deadline duration.
func DeadlineDurationAnnotationKey(userID string) string {
	return fmt.Sprintf("%s/%s", userID, DeadlineDurationSuffix)
}

// ExecutionDurationAnnotationKey returns the annotation key for execution duration.
func ExecutionDurationAnnotationKey(userID string) string {
	return fmt.Sprintf("%s/%s", userID, ExecutionDurationSuffix)
}
