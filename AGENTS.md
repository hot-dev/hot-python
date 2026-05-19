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
pytest
python -m build
```

## CI and Releases

- CI runs on pushes and pull requests to `main`: install, lint (`ruff`), test
  (`pytest`), and build (`python -m build`) on Python 3.10–3.13.
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
