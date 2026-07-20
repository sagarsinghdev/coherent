# Contributing to coherent

Thanks for your interest in improving `coherent`. Contributions of all kinds are welcome —
bug reports, documentation, tests, and code.

## Ground rules

- **Open an issue first** for anything beyond a small fix, so we can agree on the approach before you
  invest time.
- Keep pull requests **focused** — one logical change per PR.
- Every behavioural change needs **tests**. Bug fixes should include a test that fails without the fix.
- Public API changes must update **godoc** and, where relevant, the `README` and `CHANGELOG`.

## Developer workflow

```sh
make fmt     # gofmt
make vet     # go vet
make race    # go test -race ./...
make lint    # golangci-lint (see .golangci.yml)
make bench   # benchmarks
make check   # everything CI runs
```

CI runs formatting, `go vet`, race-enabled tests with coverage, and golangci-lint on supported Go
versions. PRs must be green.

## Design principles

- **Small interfaces.** `Cache`, `InvalidationSource`, and `Handler` are intentionally minimal; keep
  them that way.
- **Zero required dependencies in the core.** Transport- and backend-specific code (gRPC, Kafka,
  Otter) lives in `examples/` or `contrib/` with its own module, never in the root module.
- **Correctness at the edges.** Preserve the two invariants: clear-on-reconnect and idempotent,
  key-precise eviction; and register-before-replay on the server side.

## Commits & DCO

Write clear commit messages. By contributing, you agree your contributions are licensed under the
project's [Apache-2.0](LICENSE) license.
