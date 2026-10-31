# AGENTS.md - hot-python

This repo contains the Python SDK for the Hot Dev API.

## Package Shape

- PyPI distribution name: `hot-dev`
- Python import package: `hot`
- Source layout: `src/hot/`, with `py.typed` marker
- Runtime dependency: `httpx`
- Versioning follows the public API package version, starting at `1.0.0`.

## Commands

```bash
python -m pip install -e ".[dev]"
ruff check .
ruff format --check src tests
pytest
python -m build
```

## CI and Releases

- CI runs on pushes and pull requests to `main`: install, lint (`ruff check`,
  `ruff format --check`), test (`pytest`), and build (`python -m build`) on
  Python 3.10–3.15. Dev tools are pinned in `pyproject.toml`; Dependabot bumps
  them and the Actions weekly.
- The "Org API isolation (PostgreSQL)" job builds the hot revision in
  `integration/hot-server.ref` (keep the CI checkout `ref:` identical) and runs
  `integration/suborg_fixture.py` against restricted PostgreSQL roles. A weekly
  scheduled run builds hot `main` instead, to catch API drift.
- PyPI releases run from tags named `v<version>` matching the
  `pyproject.toml` version.
- Publishing uses PyPI Trusted Publishing via OIDC against the `pypi` GitHub
  Actions environment — no PyPI token secret is required, but the project
  publisher must be configured on PyPI before the first tag push.

Release flow:

```bash
# Bump the version in pyproject.toml.
git add pyproject.toml
git commit -m "release v<version>"
git tag v<version>
git push origin main v<version>
```

Always update the SDK version in lockstep with new API surface area added to
the matching `@hot-dev/sdk` JavaScript release when possible.

## Integration Tests

`integration/run.sh` starts a scratch `hot dev` (requires the `hot` CLI and
python3 on PATH) serving the Hot fixture project in `integration/fixture`
(project `sdk-fixture`: an echo event handler, an always-failing handler and
function, and hot:call targets). It binds 127.0.0.1 on HOT_TEST_PORT (default
4725), mints a full-access API key with `hot key create`,
exports HOT_TEST_BASE_URL / HOT_TEST_API_KEY, and runs `pytest tests/test_integration.py`.
The integration tests skip when HOT_TEST_API_KEY is unset, so regular test
runs are unaffected. The fixture pins the API to 127.0.0.1 because binding
"localhost" can yield an IPv6-only listener that JDK-style clients (which
dial the first resolved address only) cannot reach.

## Lockstep Releases

All five SDK repos (hot-js, hot-python, hot-go, hot-rust, hot-java) release in
lockstep. Checklist for a coordinated release:

1. Bump every SDK's version: js `npm version` (syncs src/version.ts via the
   `version` script), python pyproject.toml, rust Cargo.toml, java
   gradle.properties + README snippets, go `Version` constant in client.go.
2. Regenerate each repo's openapi-operations fixture from a current `hot dev`
   and reconcile the coverage tables.
3. Run every repo's `integration/run.sh`.
4. Commit, tag `v<version>`, push in each repo.

The integration fixture (integration/fixture, run.sh) is
duplicated across all five repos; the canonical copy lives in **hot-python** —
change it there first, then sync the others.
