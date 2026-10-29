# Lab 1 — Reflection-Based RPC (Go)

A small RPC system: a TCP server that calls exported methods by name using
reflection, and a client that sends JSON requests. One connection = one call.

## Layout
- `rpc/rpc.go` — Server (Start, Address, Stop, handle, invoke) and Client.Call
- `rpc/rpc_test.go` — tests
- `cmd/ping/main.go` — sample program

## Run
From this folder (the one with `go.mod`):

```sh
go test ./...
go vet ./...
go run ./cmd/ping
```
