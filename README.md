# replay

Replay is a robust durable asynchronous distributed application logic framework. 

Inspired by [temporal](https://www.temporal.io) but implemented using reflex and mysql, it 
uses the event sourcing pattern combined with event streams resulting in fault-oblivious logic.

- Robust: Automatic retries on all failures, makes it fault-oblivious to all temporary errors. 
- Durable: All state changes are automatically persisted in the event log, allowing long running stateful logic across restarts.
- Asynchronous: All communication happens asynchronously via the event log
- Distributed: Input, output, and logic execution are decoupled and can be performed by different processes.
- Application logic: Arbitrary logic is supported as long as workflows are deterministic and activities are idempotent.  

## Concepts 

Replay SDK enable you to build applications around a set of key concepts.
                                             
- Workflows: Deterministic functions forming the entry points to units of application logic.
- Activities: Functions that handle non-deterministic business logic. Accepting read-write calls from workflows.
- Runs: Invocations of workflow functions with specific input resulting in calls to activities and/or outputs.
- Signals: Write-only calls to workflow runs that the workflow can listen for and react on.
- Outputs: Write-only calls emitted by workflow runs that application logic can consume.

## Notes
- A workflow stitches together a bunch of activities within a logical flow.
- An activity takes and argument, executes arbitrary logic (including side effects) and returns a result.
- Outputs are data emitted by workflow runs. It allows workflows to publish data that application logic can subscribe to.
- Signals are data send to workflow runs by application logic. It allows workflows to react to external events.   
- A workflow function supports calling activities with arbitrary logic flow (if/for/sleep) but has the following limitations
  - It must be deterministic (no usage of rand, time packages)
  - It may not have side effects (no usage of log, backends packages)
- Side effects must be limited to activities or outputs.

> Outputs vs Activities: Both activities and outputs can be used by workflows to trigger business 
> logic with data. 
> An activity's input, logic and output are tightly coupled with a workflow (think function calls). 
> While an output is only data emitted by a workflow decoupled from consuming logic (think pub/sub).
> Another big benefit of outputs are that they do not impact workflow determinism in replay; that 
> means outputs may be added to, reordered in, or removed from, active runs. It is therefore recommended
> to use outputs over activities where possible.

- A workflow is similar to the golang function definition: `func workflowFoo(context.Context, args proto.Message)`.
- An activity is similar to the golang function definition: `func doSomething(context.Context, Backends, proto.Message) proto.Message, error`
- A run is an invocation of that function with an argument: `workflowFoo(ctx, &Bar{Field:"baz"})`
- The replay framework retries all activities indefinitely until no error is returned. 
- Application logic errors should be returned in the result proto.  
- Runs are robust to all temporary types of failure. They continue where they left off.
- Activities should be idempotent, since they may be called twice for the same invocation.
- Workflows, activities, outputs and signals are defined within a namespace and should be unique in the namespace.
- Signals is a way for external systems to send data/notifications to a workflow run.
- Signals is an enum and are workflow and run specific.
- Signals are consumed in the order they are received.
- Multiple signals of the same type can be sent to the same run.
- The workflow must check for signals, there is no guarantee that a workflow will consume all signals.

## Safety

The `replay.Client` and `replay.RunContext` APIs are not type safe. It up to the user of the replay framework
to ensure that protobuf message types (input and output) passed to and returned from activities, signals, outputs and runs 
match those defined in the actual code.

The names of workflows, activities and outputs must also match and may not change while runs are active. 
Renaming functions is therefore not allowed once used in production, except if explicit name overrides are used 
via `replay.WithName`.

The `typedreplay` code generation tool is provided as a way to mitigate the above risks. It is an opinionated
code generation wrapper of the replay framework that generates a strongly typed API based on a structured 
input definition. 

A `typedreplay.Namespace` structure should be defined in code and includes all workflows, signals, outputs and activity 
names and types. The `typedreplay` command can then be run by the `//go:generate typedreplay` directive in the same file.

A `replay_gen.go` file will then be generated that provides a type-safe API for that namespace and workflows.
A type-safe workflow can then be implemented using the `{workflow}Flow` interface. Type-safe run, 
signal, stream and output handling functions are also generated.

`typedreplay` also provides the following benefits:
- decoupling names stored in the DB from function names making renaming function safe.
- all types and names are explicitly defined so that breaking changes to them are easier to prevent. 
- unit testing workflow functions can be done using standard interface mocking.

See these test file for examples: 
 - User code: [replay.go](./typedreplay/internal/testdata/replay.go)
 - Generated code: [replay_gen.go](./typedreplay/internal/testdata/replay_gen.go)

Build to typedreplay tool with:
```
go install github.com/corverroos/replay/typedreplay/cmd/typedreplay
```

## Getting started

The best place to get started is the [replaytutorial](https://github.com/corverroos/replaytutorial).

## Under the hood

Replay uses [event sourcing](https://www.eventstore.com/event-sourcing) to decouple writes and reads and to transform seemingly synchronous workflows
into robust asynchronous fault-oblivious logic.  

This diagram provides a quick overview of the event sourcing paradigm:
```
 Event Log:
  - append-only persistance of events for a long time (days/months/years)
  - events are ordered, each event has a offset/position/id
  - provides API for streaming events to consumers
  - supports dedup on insert API
  - primary source of trouth
  - record of everything that happended over time

   ┌─────────────────────┐
 ┌─►      event log      ├────┬──────┬──────┐
 │ └─────────────────────┘    │      │      │
 │                         ┌──▼─┐ ┌──▼─┐ ┌──▼─┐
 │                         │ c1 │ │ c2 │ │ c3 │
 │                         └──┬─┘ └──┬─┘ └──┬─┘
 │                            │      │      │
 └────────────────────────────┴──────┴──────┘

  Consumers:
   - consume all events sequentially
   - decoupled, since maintain own cursor/offset
   - cursor persisted somewhere (in some DB)
   - continues from previous cursor on restart
   - delivery is AT-LEAST-ONE, so logic must be idempotent
   - output events back to the event log
   - no direct consumer to consumer comms, only events via the log
   - given deterministic logic, can produce state at any point in time.
```

Instead of having to deal with events and consumers and streams, replay abstracts that away. You define normal "workflow" and "activity" functions, register them with replay, and lets the framework handle the rest of translating your synchronous code into event sourcing. 

Below is pseudocode for a workflow function and its associated underlying events that are generated by replay. Note `->` denotes consumed events and `<-`  produced events. 
```
// TWAPWorkflow is a time-weighted-average          | 
// order strategy that creates periodic             | 
// sub-orders.                                      |
func TWAPWorkflow(req):                             | -> run_create{TWAPWorkflow, req}
  for i in req.Splits:                              | # local logic   
    price = orderbooks.CurrentTop(req.Market)       | <- activ_req{orderbooks.CurrentTop, market}
                                                    | -> activ_res{orderbooks.CurrentTop, price}
    subOrder = makeOrder(req, i, price)             | # local pure function
    publishOrder(subOrder)                          | -> run_output{orders.publish, order}
    sleep(calcPeriod(req, i))                       | -> sleep_req{duration}
                                                    | <- sleep_res{}
                                                    | ... repeat N times 
```
Below is pseudocode for an activity function and its associated underlying events that are generated by replay.
```
// OrderBooks tracks and maintians live 
// market data state.
type OrderBooks struct {
  ... 
}

// CurrentTop returns the current top 
// price for the provided market.
func (o OrderBooks) CurrentTop(market):             | -> activ_req{orderbooks.CurrentTop, market}
  return o.markets[market].Top()                    | <- activ_res{orderbooks.CurrentTop, price}   
```

The diagram below shows an overview of the replay architecture. 
It contains the following elements:
- `replay server binary`: Serves the replay grpc API (for both internal client `IntCl` and replay client `RepCl`) that allows inserting events into the event log as well as streaming events from it. It also runs the sleep and signal logic.
- `user app binary`: The user application binary that is using the replay framework/sdk/package and which connects to the replay server vir grpc. 
- `replay sdk/pkg`: Provides the replay client `RepCl` available to the user. Runs event consumers for the user's registered workflows and activities `WC`,`AC`.
- `event log`: Append only reflex mysql event table storing all replay events.
- `stream`: reflex event stream of the event log, provided by replay server grpc API and available in replay client `RepCl`.
- `server`: package providing the grpc server API `gSrv` for the internal client `IntCl` and replay client `RepCl`. It also provides the server side DB client `DBCl` that is responsible for all actual DB calls. Note that the DB client also implements the internal and replay clients, so can be used to embed replay in the user application to avoid the need for an actual remote grpc server. 
- `sleep`: Logic and database tables implementing the sleep feature.  
- `signal`: Logic and database tables implementing the signal feature. Note that `SignalRun` inserts rows into signal feature tables and not directly into the event log. Only once a signal is matched with an AwaitSignal call, is an event inserted.
- `IntCl`: Internal client used by the replay sdk to insert and query events.
- `RepCl`: Replay client used by the user to run workflows and signal runs. 
- There are only 5 replay event types (only applicable to the replay sdk):
  - `cre`: `RunCreated` is inserted to start a workflow run iteration; `user->RC`. It contains the workflow input message.
  - `req`: `ActivityRequest ` indicates a worfklow run has requested an activity; `wfunc->RG->IC`. It contains the activity input message.   
  - `res`: `ActivityResponse ` indicates a response from an activity; `afunc->AC->IC`. It contains the activity output messagean activity.
  - `com`: `RunCompleted` indicates a workflow run has completed `RG->IC`.   
  - `out`: `RunOutput` indicates an output from a workflow run `wfunc->RG->IC`.   
- `AC`: Activity consumer is a reflex event consumer. It consumes activity request `req` events, executes the activity function `afunc` and inserts activity responses `res` in the event log via the internal client `IntCl`. It is started by the user calling `RegisterActivity` on the replay sdk.
- `WC`: Workflow consumer is a reflex event consumer. It consumes create run `cre` and activity reponse `res` events. It manages multiple run goroutines `RG`, one per run and passes the events them. It is started by the user calling `RegisterWorkflow` on the replay sdk.
- `RG`: Run goroutine is the actual goroutine executing the workflow function `wfunc`. It is managed by, and receives all input from, the workflow consumer `WC`. When the workflow function calls `ExecActivity`, it inserts an activity request `req` in the event log via the internal client `IntCl` and awaits a response `res` from the workflow consumer `WC`. When the workflow function returns, it inserts a run complete event `com`.   
- `wfunc`: User defined workflow function.  
- `afunc`: User defined activity function.  
- `cfunc`: User defined reflex consumer function.
  
```
                             user app binary 
replay server binary        ┌──────────────────────────────────────────────────────┐
┌────────────────────┐      │  replay SDK/pkg                                      │
│   ┌─────────┐stream│      │ ┌───────────────────────────────┐                    │
│ ┌►│event log│════> │      │ │                req            │                    │
│ │ └─────────┘      │      │ │      req  ┌──┐ ───►afunc()──┐ │                    │
│ │                  │ grpc │ │     ════> │AC│ res          │ │◄──RegisterActivity │
│ │    server        │◄────►│ │          ┌┴──┘ ◄────────────┘ │     afunc()        │
│ │ ┌────┬────┐      │      │ │          │res                 │                    │
│ └─┤DBCl│gSrv│◄─... │      │ │          ▼                    │                    │
│   └────┴────┘      │      │ │      ┌─────┐          ┌─────┐ │◄──RunWorkflow      │
│    ▲               │      │ │ ...◄─┤IntCl│     ...◄─┤RepCl│ │◄──SignalRun        │
│    │ ┌──────┐      │      │ │      └─────┘          └─────┘ │──►Stream────┐      │
│    ├─┤sleep │<══╝  │      │ │          ▲req                 │           cfunc()  │
│    │ └──────┘      │      │ │          │com  cre            │◄─►Handle────┘      │
│    │               │      │ │ cre      └┬──┐ ───►wfunc()──┐ │                    │
│    │ ┌──────┐      │      │ │ res ┌──┐  │RG│ req/res ▲    │ │                    │
│    └─┤signal│<══╝  │      │ │ ═══>│WC├─►│  │ ◄───────┘    │ │◄──RegisterWorkflow │
│      └──────┘      │      │ │     └──┘  │  │ com/out      │ │     wfunc()        │
└────────────────────┘      │ │           └──┘ ◄────────────┘ │                    │
                            │ └───────────────────────────────┘                    │
                            └──────────────────────────────────────────────────────┘
```
