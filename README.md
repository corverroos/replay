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
- Runs are robust to all temporary types of failure. They continue where they left off.
- Activities should be idempotent, since they may be called twice for the same invocation.
- Workflows and activities are defined within a namespace and should be unique in the namespace.
- Signals is a way for external systems to send data/notifications to a workflow run.
- Signals is an enum and are workflow and run specific.
- Signals are consumed in the order they are received.
- Multiple signals of the same type can be sent to the same run.
- The workflow must check for signals, there is no guarantee that a workflow will consume all signals.

## Safety

The `replay.Client` and `replay.RunContext` APIs are not type safe. It up to the user of the replay framework
to ensure that proto messages (input and output) passed to and returned from activities, signals and runs 
match those defined in the actual code. 

The names of workflows, activities and signals must also match and may not change while runs are active. 
So renaming functions is not possible once used in production, except if explicit name overrides are used 
via `replay.WithName`.

The `replaygen` code generation tool is provided as a way to mitigate the above risks. It is an opinionated
code generation wrapper of the replay API. 

A `replaygen.Namespace` structure is defined in code and includes all workflows, signals and activities names and types.
The `replaygen` command is then run by the `//go:generate replaygen` directive in the same file.

A `replay_gen.go` file will then be generated that provides a type-safe API for that namespace and workflows.
A type-safe workflow can then be implemented using the `{workflow}Flow` interface. Type-safe run and 
signals functions are also generated.

`replaygen` also provides the following benefits:
- decoupling names stored in the DB from function names making renaming function safe.
- all types and names explicit so that breaking changes to them are easier to detect and block. 
- unit testing workflow functions can be done using standard interface mocking.

See these test file for examples: 
 - User code: [replay.go](./replaygen/internal/testdata/replay.go)
 - Generated code: [replay_gen.go](./replaygen/internal/testdata/replay_gen.go)

Build to replaygen tool with:
```
go install github.com/corverroos/replay/replaygen/cmd/replaygen
```

