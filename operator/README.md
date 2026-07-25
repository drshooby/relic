# daemon

Go operator that tails the Warframe `EE.log` and ships events downstream.

## Prerequisites

- Go 1.25+

## Build

From this directory:

```sh
go build -o tail ./cmd/tail/
```

## Run

Without building first:

```sh
go run ./cmd/tail/
```

Or after building:

```sh
./tail
```
