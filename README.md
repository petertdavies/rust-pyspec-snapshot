# Rust Pyspec Snapshot Tool

This tool generates snapshots in the format used by the python based
[Ethereum Execution Specs](https://github.com/ethereum/execution-specs).

It works by syncing the Ethereum chain to the requested block using
[Akula](https://github.com/akula-bft/akula) and then extracting the state from
Akula's internal database. Block headers and bodies are extracted from the local
Erigon database, so you must have an installation of Erigon that has downloaded
the headers and bodies you need.

By default, a different datadir to vanilla Akula is used so you can run both on
the same machine if needed.
