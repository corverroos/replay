# replay

Replay is a PoC workflow framework inspired by [temporal](www.temporal.io) but implemented using a mysql reflex events.

See [TestExample](./example/example_test.go) for an overview of the replay API.

## Notes
- A workflow stitches together a bunch of activities within a logical flow.
- An activity takes and argument, executes arbitrary logic (including side effects) and returns a result.
- A workflow is similar to the golang function definition: `func workflowFoo(context.Context, args proto.Message)`.
- An activity is similar to the golang function definition: `func doSomething(context.Context, Backends, proto.Message) proto.Message, error`
- A run is an invocation of that function with an argument: `workflowFoo(ctx, &Bar{Field:"baz"})`
- A workflow function supports calling activities with arbitrary logic flow (if/for/sleep) but has the following limitations
  - It must be deterministic (no usage of rand, time packages)
  - It may not have side effects (no usage of log, backends packages)
- Side effects must be limited to activities.
- The replay framework retries all activities indefinitely until no error is returned. 
- Application logic errors should be returned in the result proto.  
- Run are robust to all types of failure. They continue where they left off.
- Activities should be idempotent, since they may be called twice for the same invocation.
- Signals is a way for external systems to send data/notifications to a workflow run.
- Signals is an enum and are workflow and run specific.
- Signals are consumed in the order they are received.
- Multiple signals of the same type can be sent to the same run.
- The workflow must check for signals, there is no guarantee that a workflow will consume all signals.

## TODO

- Add type check test.
