package replaypb

//go:generate protoc --proto_path=. --proto_path=../../../protos --go_out=. ./replay.proto
//go:generate protoc --proto_path=. --proto_path=../../../protos --go-grpc_out=. ./replay.proto
