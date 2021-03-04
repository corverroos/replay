# replay

Replay is a PoC workflow framework inspired by [temporal](www.temporal.io) but implemented using a mysql reflex events.

See [TestExample](./example/example_test.go) for an overview of the replay API.

## TODO

- Add replayability: When a workflow is stopped in the middle, it should continue where it left off.
- Add support for custom proto types to workflow/activity signatures. 
