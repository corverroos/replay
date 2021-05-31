// Package typedreplay provides the input definitions for the typedreplay command that generates
// a typed replay API based on the input.
package typedreplay

import "github.com/golang/protobuf/proto"

// Namespace groups workflows and activities.
type Namespace struct {
	// Name of the namespace. This may never change.
	Name string

	// Workflows of the namespace.
	Workflows []Workflow

	// Activities of the namespace.
	Activities []Activity

	// ExposeRegisterFuncs generates exported Register functions for all workflows and activities
	// as opposed to the default single startReplayLoops function. This allows more control over
	// the how and where the workflow and activity consumers are executed.
	ExposeRegisterFuncs bool
}

// Workflow defines a workflow to generate.
type Workflow struct {
	// Name of the workflow. This may never change.
	Name string

	// Description of the workflow.
	Description string

	// Input is the input parameter proto type. This may never change.
	Input proto.Message

	// Signals of the workflow. Signals are activities, so they may also only be appended to workflow logic.
	Signals []Signal

	Outputs []Output
}

// Signal defines a signal.
type Signal struct {
	// Name of the signal.
	Name string

	// Description of the signal.
	Description string

	// Enum is the signal enum (type). This may never change.
	Enum int

	// Message is the protobuf message of the signal. This may never change.
	Message proto.Message
}

// Output defines a workflow output.
type Output struct {
	// Name of the signal. This may never change.
	Name string

	// Description of the signal.
	Description string

	// Message is the protobuf message of the output. This may never change.
	Message proto.Message
}

// Activity defines an activity.
type Activity struct {
	// Name of the activity. This may never change.
	Name string

	// Description of the activity.
	Description string

	// Func is the activity function.
	Func interface{}
}
