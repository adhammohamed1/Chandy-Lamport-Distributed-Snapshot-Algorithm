# Chandy-Lamport-Distributed-Snapshot-Algorithm
The Chandy-Lamport distributed snapshot algorithm is designed to capture a consistent global state of a distributed system, which is useful for debugging, checkpointing, and ensuring data consistency. The algorithm works by having an initiator process record its local state and send a special marker message to all other processes. Upon receiving a marker for the first time, each process records its local state and sends the marker to its neighbors, while also logging all incoming messages on each channel until a marker is received on that channel. This way, the algorithm captures a snapshot of the system's state without disrupting its ongoing operations.

## Overview

This repository contains an implementation of the Chandy-Lamport distributed snapshot algorithm in GoLang. The Chandy-Lamport algorithm is used to record a consistent global state of a distributed system. It is particularly useful for debugging, checkpointing, and ensuring consistency in distributed systems.

## The Problem

In a distributed system, processes run concurrently on different nodes and communicate by passing messages. Recording the global state of such a system is challenging because there is no global clock, and the processes do not share a common memory. The global state consists of the local states of all processes and the state of all communication channels.

A consistent global state is one that could have occurred at some instant during the system's execution. The Chandy-Lamport algorithm ensures that the snapshot is consistent and can be used for various purposes, such as recovery from failures or analyzing the system's behavior.

## The Algorithm

The Chandy-Lamport algorithm works as follows:

1. **Initiation**: Any process in the system can initiate the snapshot. This process, called the initiator, records its local state and sends a special marker message along all its outgoing channels.

2. **Recording State**:
    - Upon receiving a marker for the first time, a process records its local state and sends the marker along all its outgoing channels.
    - The process then starts recording all incoming messages on each channel until a marker is received on that channel.

3. **Completing the Snapshot**:
    - Once a process has received a marker on all its incoming channels, it stops recording the messages.
    - The recorded state consists of the local state and the messages recorded on each channel.

The global state is the combination of all recorded local states and the state of all channels.

## Repository Contents

The repository contains the following files and directories:

- `test_data/`: Directory containing test data used for validating the algorithm.
- `common.go`: Go source file with common utility functions and data structures used across the implementation.
- `go.mod`: Go module file defining the module's path and dependencies.
- `logger.go`: Go source file implementing logging functionalities for the distributed system.
- `node.go`: Go source file defining the `Node` structure and methods, representing a process in the distributed system.
- `queue.go`: Go source file containing implementation of a queue data structure used for message passing between nodes.
- `sim.go`: Go source file responsible for simulating the distributed system and running the snapshot algorithm.
- `snapshot_test.go`: Go source file with tests for the Chandy-Lamport distributed snapshot algorithm implementation.
- `test_common.go`: Go source file with common functions and data structures used in tests.

