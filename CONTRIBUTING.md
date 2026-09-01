# Contributing

Pulse takes patches through pull requests against `master`.

## Before you open a pull request

Run the full gate locally and make sure it is green:

```
make fmt-check vet lint test-race
```

There is no Go toolchain requirement beyond what `go.mod` names; if you do
not have Go installed, run the same targets inside `golang:1.26` with the
repository mounted as `/src`.

## Commit style

Commits follow Conventional Commits: `feat(scope): subject`,
`fix(scope): subject`, `refactor(scope): subject`, `test(scope): subject`,
`docs: subject`, `chore(scope): subject`. The subject is imperative, lower
case, and has no trailing period. Keep one concern per commit.

## Pull requests

Describe what changed and why, not just what. Link the issue it closes, if
any. Small, focused pull requests review faster than large ones.

## Code of conduct

Be respectful and assume good faith. Disagreements about code are fine;
personal attacks are not.
