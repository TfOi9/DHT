# Distributed Hash Table - PPCA 2026

## Overview

A Distributed Hash Table (DHT) is a distributed system that provides a lookup service similar to a hash table: `(key, value)` pairs are stored across many nodes, and any participating node can efficiently retrieve the value associated with a given key. The goal is to store and retrieve data in a scalable, efficient and reliable manner.

There are many algorithms to implement DHT. For this project, you are required to **implement Chord protocol and Kademlia protocol**. You should **write a report for about one page**, probably about your architecture, innovation, features and references. 

As a bonus, you can also 
- implement an application of DHT (e.g., File-Transfer or Group-Chat-Room under local area network).
- Append an implementation of the Delete() in Kademlia. 

## Project Layout

```
.
├── main.go                  CLI entry point: interactive REPL + optional TCP command server
├── go.mod / go.sum          module `dht`, Go 1.18
├── node/                    core
│   ├── interface.go         the DhtNode interface
│   ├── node.go              Protocol - you should replace its implementation with Chord/Kademlia on different branches
│   ├── factory.go           NewNode(port)
│   ├── addr.go              local-address / port→addr helpers
│   ├── basic_test.go        TestBasic
│   ├── advance_test.go TestForceQuit, TestQuitAndStabilize   
│   └── delete_test.go      TestDelete
├── testutil/
│   └── helpers.go           shared test constants, colored output, pass/fail metrics
└── doc/                     documentation (setup & tutorial)
```

## Build and Run

Build the binary from the project root:

```bash
go build -o dht .
```

Run a single node and start the ring with `Create`:

```bash
./dht -port 20000
```

Run another node that joins an existing one:

```bash
./dht -port 20001 -join 127.0.0.1:20000
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `-port` | `20000` | Port the node listens on for RPC. |
| `-addr` | `127.0.0.1` | Address advertised to other nodes (use a reachable host name when running across multiple machines). |
| `-join` | _(empty)_ | Address of an existing node to join. If empty, the node creates a new ring. |
| `-cmd-port` | `0` | Port for a line-based TCP command server (`0` disables it). Lets you drive a node over TCP, e.g. for scripting a multi-node demo. |

### Interactive commands

Once running, a node reads commands from standard input (and, if enabled, from
the `-cmd-port` TCP server). One command per line:

```
put <key> <value>     # store a pair, prints "true" / "false"
get <key>             # look up a key, prints the value or "false"
delete <key>          # remove a key, prints "true" / "false"
quit                  # gracefully leave the ring and exit
```

## Tutorial

First, you should read the [Environment Setup](doc/env-setup.md) to setup your environment.

A naive implementation of `dhtNode` is provided in `node/node.go`. You can use it as a reference. The code is well commented. It is suggested to **read it carefully**.

You can read the [Tutorial](doc/tutorial.md) for more information about Go, DHT and how to debug.

## Scores

- 40% for the Chord Test
  - 30% Basic test: naive test without "force quit".
  - 10% Advance test: "Force quit" will be tested. There will be some more complex tests.
- 40% for the Kademlia Test (Same as above)
- 20% for a short report and code review
- Extra 10% for the bonus

## Testing

Note: **DHT tests cannot run successfully under Windows or WSL 1**. See [Environment Setup](doc/env-setup.md) for more information.

The four in-process tests (`TestBasic`, `TestForceQuit`, `TestQuitAndStabilize`, `TestDelete`) each use a separate, reserved port range, so you can run them together with `go test ./node/...` — or run them one at a time with `go test ./node -run TestBasic -v`.

Attention: The tests can take longer than Go's default 10‑minute timeout, so **always disable the timeout when running these tests:**

```bash
go test ./node -run TestBasic -v -timeout 0
```

(Use `-timeout 0` to turn off the limit entirely, or a generous value like `-timeout 30m`.)

Contact TA if you find any bug in the test program, or if you have some test ideas, or if you think the tests are too hard and you want TA to make it easier.

### Basic Test

There are **5 rounds** of test in total. In each round,

1. **20 nodes** join the network. Then **sleep for 10 seconds.**
2. **Put 150 key-value pairs**, **query for 120 pairs**, and then **delete 75 pairs**. There is **no sleep time between two contiguous operations**.
3. **10 nodes** quit from the network. Then **sleep for 10 seconds**.
4. (The same as 2.) **Put 150 key-value pairs**, **query for 120 pairs**, and then **delete 75 pairs**. There is **no sleep time between two contiguous operations**.

### Advance Test

The advance test consists of "**Force-Quit Test**" and "**Quit & Stabilize Test**".

#### Force-Quit Test

The current test procedure is:

* In the beginning, **50 nodes** join the network.
* Then **put 500 key-value pairs**.
* It follows by **9 rounds** of force quit. In each round,
  1. **5 nodes force-quit** from the network. There is **500ms of sleep time** between each force-quit operation.
  2. **Query for all key-value pairs**.

#### Quit & Stabilize Test

The current test procedure is:

* In the beginning, **50 nodes** join the network.
* Then **put 500 key-value pairs**.
* Next, **every node will quit from the network**:
  1. One node quits.
  2. After the node quitting from the network, there is **80ms of sleep time**. And then **20 key-value pairs will be queried for**.

### Delete Test (Bonus)

The delete test focuses on the **removal of key-value pairs** from the nodes. The current test procedure is:

* In the beginning, **21 nodes** join the network.
* Then **put 200 key-value pairs**.
* **Delete every key-value pair**. Each deletion of an existing key must **succeed**.
* **Query for every deleted key**. None of them should be found any more.
* **Delete the already-removed keys again**. Each of these deletions must **report failure**.

