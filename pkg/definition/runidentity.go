package definition

// RunIdentity identifies one run (one scenario, one iteration) in the transition record
// (research.md section 11.5). The evaluator cannot derive this itself: it does not know
// how the input was produced or how the scheduler profile was configured, so the caller
// (the program that runs the evaluator, see decisions.md) supplies it.
type RunIdentity struct {
	TraceName        string  // The name of the input trace, e.g. "openb".
	TraceVersion     string  // The version of the input trace.
	ReferenceCommit  string  // The commit of the reference repository that both produced the input and is running this evaluation.
	InputSeed        string  // The seed used to produce this input version.
	Rho              float64 // The target load factor (ρ) of this input version.
	SchedulerProfile string  // A description of the scheduler profile configuration used for this run.
}
