# Org API SDK validation

SDK 1.3.0 wraps hot's org API (`/v1/orgs`, `/v1/api-keys` and the org-qualified
project and build routes) on the normal client. It replaces the unreleased
`HotManagementClient`, which targeted the `/v1/management` API and `hm_` keys
that hot removed before 3.0.1. Matching server revision:
`hot-dev/hot@d5dde712f447daf88c38e5546f805496c46a2877` (main, 2026-10-30).

Each SDK's pull request records its local validation against this pin: unit and
contract checks, `integration/run.sh`, and this fixture on SQLite and PostgreSQL.
Every fixture run compares the SDK's coverage fixture with all **83 live
OpenAPI operations** of the pinned server.

## The fixture

`integration/suborg_fixture.py` is identical in all five SDK repos (canonical
copy: hot-python). It runs `hot init` on a disposable SQLite file or a
PostgreSQL 18 container, then:

- seeds a top org API key (`org_api_key`, as a root administrator would create
  in the App) and an independent root environment key;
- starts `hot api` with a random master key (`HOT_ENCRYPTION_KEY`) and, for
  PostgreSQL, a Valkey container for SSE ingress;
- checks the live `/openapi.json` against the SDK's operations fixture;
- mints a delegated org key through `POST /v1/orgs/{root}/api-keys`, uses it to
  create a child org and an environment key, issues a service key and a
  session from that key, then revokes or narrows the delegated key.

The SDK tests receive `HOT_TEST_SUBORG_FIXTURE` (`root_org_id`, `root_env_id`,
`root_token`, `org_token`, `revoked`, `narrowed`) and check: child and
environment provisioning and its retry safety, environment-key minting,
org-scoped projects and context, sibling isolation, `key_scope_mismatch`
in both directions as the typed error, and that credentials issued under a
revoked or narrowed org key stop or narrow on their next request.

PostgreSQL runs migrations with a non-superuser schema owner and serves the
API with a separate `NOSUPERUSER NOBYPASSRLS` login, with RLS enforced.
`--pg-port` picks the container's host port. The fixture removes its
database, API process and containers on exit, and passes the disposable
credentials only to the test subprocess.

## CI

The `Org API isolation (PostgreSQL)` job builds the server revision pinned in
`integration/hot-server.ref` and runs the fixture on pull requests and pushes
to main. A weekly scheduled run builds hot `main` instead, to catch API drift
early. When the API changes, update `integration/hot-server.ref` and the CI
checkout SHA together, regenerate the OpenAPI fixture, and rerun the fixture.
Keep the pin aligned across all five SDK repos. After hot v3.0.1 is tagged,
pin to the tag's commit. The `Integration (hot dev)` job still tests the
runtime API against the released Hot CLI.
