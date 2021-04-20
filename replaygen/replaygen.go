package replaygen

import "github.com/golang/protobuf/proto"

// Activity defines an activity.
type Activity struct {
	// Name of the activity.
	Name string

	// Description of the activity.
	Description string

	// Func is the activity function.
	Func interface{}
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
}

// Namespace groups workflows and activities.
type Namespace struct {
	// Name of the namespace. This may never change.
	Name string

	// Workflows of the namespace.
	Workflows []Workflow

	// Activities of the namespace.
	Activities []Activity
}
