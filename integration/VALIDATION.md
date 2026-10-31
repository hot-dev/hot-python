# Sub-org SDK validation

Validated locally on 2026-10-20 for the coordinated SDK 1.3.0 changes on
`codex/sub-org-management-sdk`. Matching public server revision:
`hot-dev/hot@d5dde712f447daf88c38e5546f805496c46a2877`.

| SDK | Unit/contract checks | Existing runtime integration | Sub-org SQLite | Sub-org PostgreSQL |
| --- | --- | --- | --- | --- |
| JavaScript/TypeScript | 38 passed; typecheck and package build passed | 5 passed | 2 passed | 2 passed |
| Python | 31 passed; Ruff and wheel/sdist build passed | 6 passed | 3 passed | 3 passed |
| Go | 32 top-level tests plus 10 subtests passed; vet and race checks passed | 5 passed | 2 passed | 2 passed |
| Rust | 30 passed including doctest; fmt, strict Clippy and docs passed | 5 passed | 2 passed | 2 passed |
| Java | 29 passed; build and Javadocs passed | 5 passed | 2 passed | 2 passed |

Ordinary unit runs skip the live-server tests; the integration results above
come from separate explicit runs. Each final sub-org fixture run compared the
SDK's coverage fixture against all **68 live OpenAPI operations**.

The PostgreSQL 18 fixture ran migrations with a non-superuser schema owner,
then provisioned and qualified a separate non-owner `NOSUPERUSER NOBYPASSRLS`
API login. Runtime RLS enforcement stayed enabled. Fixture setup alone used
its disposable database administrator to seed test credentials. Each fixture
owns and removes its temporary database, API process, and containers.

Sub-org coverage: retry-safe child/environment provisioning; same project names
in sibling environments; parent/sibling resource and stream denial; management
versus runtime token separation; environment usage versus shared limits; scoped
API-key grants; service/session issuance and revocation; already issued API,
service, and session credentials after issuer revocation/narrowing; and child
suspension/resumption without affecting a sibling. Python also exercises its
async clients; Java exercises its asynchronous management wrapper.

The shared fixture is canonical in hot-python and identical in all five repos.
Use the commands in each SDK README to reproduce against the matching Hot build.
Native checks used macOS arm64, Node 26.12.0, Python 3.14, Go 1.27.3,
Rust 1.99.1, and JDK 21.0.12.1. Linux and minimum-toolchain checks remain in CI.

No hierarchy changes or shared deployments are included. The server contract
remains a root with direct children, each owning its environments/deployments.

## PostgreSQL isolation in CI

The `PostgreSQL Sub-org Isolation` job in `.github/workflows/ci.yml` runs on
pull requests and main-branch pushes. It builds the server revision pinned in
`integration/hot-server.ref`, then invokes the same restricted-role PostgreSQL
fixture used locally. The job checks all live OpenAPI operations and runs the
SDK's sub-org tests explicitly, so their usual unit-test skips cannot hide a
failure. It uses only disposable local credentials and does not deploy Hot.

When the API contract changes, update the full server commit in
`integration/hot-server.ref` and the matching immutable checkout SHA in CI,
refresh the OpenAPI fixture, and rerun isolation
before committing. Keep this pin aligned across all five SDK repos. The legacy
runtime integration job remains a check against the released Hot CLI.

Validation of this CI addition on 2026-10-20: Actionlint (including ShellCheck)
passed for all five workflows. Each job's final test command also passed
locally against the restricted-role PostgreSQL fixture: JavaScript 2 tests,
Python 3, Go 2, Rust 2, and Java 2, with all 68 live OpenAPI operations checked
on every run. The five shared management examples in Hot's SDK guide compiled
and ran against that fixture as well. These were native macOS runs; hosted
GitHub Actions and Linux source builds had not yet run at initial validation.
The draft PR checks provide the hosted validation.

CI security review: third-party Actions and the server checkout use immutable
commit SHAs. The job verifies the checked-out server against `hot-server.ref`;
no step output selects executable server code. Disposable test credentials are
passed as JSON in `HOT_TEST_SUBORG_FIXTURE` to the test subprocess, rather than
written to disk. These changes preserve the restricted runtime-role assertions.

After the security fixes, the exact PostgreSQL CI commands passed again in all
five SDKs (11 tests total), as did the JavaScript/Python/Go/Rust unit checks and
workflow linting. Java recompiled and ran its changed integration tests.
