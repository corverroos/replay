package replay

import (
	"github.com/golang/protobuf/proto"
	"github.com/luno/reflex"

	"github.com/corverroos/replay/internal"
)

// Handle provides an event handler for custom replay event consumers,
// most notably workflow output consumers. It abstracts the replay encoding internals.
//
// Note that handling proto messages requires that the protobuf definitions be registered.
func Handle(e *reflex.Event, handlers ...func(*handler)) error {
	var h handler
	for _, handler := range handlers {
		handler(&h)
	}
	key, err := internal.DecodeKey(e.ForeignID)
	if err != nil {
		return err
	}

	if h.Skip != nil && h.Skip(key.Namespace, key.Workflow, key.Run) {
		return nil
	}

	if reflex.IsType(e.Type, internal.RunCreated) && h.RunCreated != nil {
		msg, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		return h.RunCreated(key.Namespace, key.Workflow, key.Run, msg)
	}

	if reflex.IsType(e.Type, internal.RunCompleted) && h.RunCompleted != nil {
		return h.RunCompleted(key.Namespace, key.Workflow, key.Run)
	}

	if reflex.IsType(e.Type, internal.ActivityRequest) && h.ActivityRequest != nil {
		msg, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		return h.ActivityRequest(key.Namespace, key.Workflow, key.Run, key.Target, msg)
	}

	if reflex.IsType(e.Type, internal.ActivityResponse) && h.ActivityResponse != nil {
		msg, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}

		return h.ActivityResponse(key.Namespace, key.Workflow, key.Run, key.Target, msg)
	}

	if reflex.IsType(e.Type, internal.RunOutput) && h.RunOutput != nil {
		msg, err := internal.ParseMessage(e)
		if err != nil {
			return err
		}
		return h.RunOutput(key.Namespace, key.Workflow, key.Run, key.Target, msg)
	}

	return nil
}

// HandleSkip returns a handler option to skip specific events.
func HandleSkip(fn func(namespace, workflow, run string) bool) func(*handler) {
	return func(h *handler) {
		h.Skip = fn
	}
}

// HandleOutput returns a handler option to consume run outputs.
func HandleOutput(fn func(namespace, workflow, run string, output string, message proto.Message) error) func(*handler) {
	return func(h *handler) {
		h.RunOutput = fn
	}
}

// HandleRunCreated returns a handler option to consume run created events.
func HandleRunCreated(fn func(namespace, workflow, run string, message proto.Message) error) func(*handler) {
	return func(h *handler) {
		h.RunCreated = fn
	}
}

// HandleRunCompleted returns a handler option to consume run completed events.
func HandleRunCompleted(fn func(namespace, workflow, run string) error) func(*handler) {
	return func(h *handler) {
		h.RunCompleted = fn
	}
}

// HandleActivityRequest returns a handler option to consume run activity requests.
func HandleActivityRequest(fn func(namespace, workflow, run string, activity string, message proto.Message) error) func(*handler) {
	return func(h *handler) {
		h.ActivityRequest = fn
	}
}

// HandleActivityResponse returns a handler option to consume run activity responses.
func HandleActivityResponse(fn func(namespace, workflow, run string, activity string, message proto.Message) error) func(*handler) {
	return func(h *handler) {
		h.ActivityResponse = fn
	}
}

type handler struct {
	Skip             func(namespace, workflow, run string) bool
	RunCreated       func(namespace, workflow, run string, message proto.Message) error
	RunCompleted     func(namespace, workflow, run string) error
	RunOutput        func(namespace, workflow, run string, output string, message proto.Message) error
	ActivityRequest  func(namespace, workflow, run string, activity string, message proto.Message) error
	ActivityResponse func(namespace, workflow, run string, activity string, message proto.Message) error
	// TODO(corver): Maybe add anypb.Any versions to support proto-unaware consumers.
}
