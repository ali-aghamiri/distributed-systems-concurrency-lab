# Concurrent Key–Value Server

A Go networking exercise for the Distributed Systems course, reconstructed from a completed implementation in my course archive. The original course assignment was adapted from Carnegie Mellon University's 15-440 Project 0. This repository contains a clean implementation and independent tests; the university handout, starter code, and tests are excluded.

The server uses one state-owning goroutine for its key–value map and client registry. TCP clients have separate reader and writer goroutines. Channels carry requests and responses, while a bounded output buffer prevents one slow reader from blocking all clients.

## Run the tests

```bash
go test ./...
go test -race ./...  # requires a C compiler and CGO
```

Requires Go 1.22 or newer. The tests use an ephemeral loopback port and do not need external services.

## Text protocol

One command per line:

```text
Put:fruit:apple
Get:fruit
Update:fruit:apple:pear
Delete:fruit
```

`Get` emits one `key:value` line per stored value. Missing keys emit no lines. Malformed commands emit `ERROR:` lines. Values and keys cannot contain colons or newlines. `Put`, `Update`, and `Delete` do not send acknowledgments. The project is a small concurrency exercise, not a durable database or production network service.
