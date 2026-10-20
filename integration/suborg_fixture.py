#!/usr/bin/env python3
"""Run SDK tests against an owned disposable sub-org server.

Canonical copy: hot-python. Requires a Hot build containing the management API.
SQLite uses only Python and Hot. PostgreSQL additionally needs Docker, psql,
and a sibling hot checkout for provision_runtime.sql. The API uses a separate
NOSUPERUSER NOBYPASSRLS runtime login; migrations use the schema owner.
Usage: python3 integration/suborg_fixture.py --backend postgres -- <test command>
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["sqlite", "postgres"], default="sqlite")
    parser.add_argument("--hot", default=shutil.which("hot"))
    parser.add_argument(
        "--hot-repo", type=Path, default=Path(__file__).resolve().parents[2] / "hot"
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not args.hot or not command:
        parser.error("--hot (or hot on PATH) and a test command are required")
    args.hot = str(Path(args.hot).resolve())
    containers = []
    server = None
    with tempfile.TemporaryDirectory(prefix="hot-sdk-suborg-") as scratch:
        work = Path(scratch)
        env = os.environ.copy()
        # Owner credentials are never passed to the API or SDK subprocess.
        for key in list(env):
            if key.startswith("HOT_DB_"):
                del env[key]
        try:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            base_url = f"http://127.0.0.1:{port}"
            admin_uri = None
            if args.backend == "postgres":
                psql = shutil.which("psql")
                if not psql:
                    parser.error("PostgreSQL fixture requires psql on PATH")
                name = "hot-sdk-suborg-" + uuid.uuid4().hex[:12]
                containers.append(name)
                password = secrets.token_hex(16)
                subprocess.run(
                    [
                        "docker",
                        "run",
                        "-d",
                        "--name",
                        name,
                        "-e",
                        "POSTGRES_PASSWORD=" + password,
                        "-p",
                        "127.0.0.1::5432",
                        "pgvector/pgvector:pg18",
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
                bound = subprocess.check_output(["docker", "port", name, "5432/tcp"], text=True)
                pgport = bound.strip().rsplit(":", 1)[1]
                admin_uri = f"postgresql://postgres:{password}@127.0.0.1:{pgport}/postgres"

                def sql(statement, uri=None):
                    return subprocess.check_output(
                        [psql, uri or admin_uri, "-X", "-qAt", "-v", "ON_ERROR_STOP=1"],
                        input=statement,
                        text=True,
                        stderr=subprocess.PIPE,
                    ).strip()

                for _ in range(120):
                    try:
                        sql("SELECT 1")
                        break
                    except subprocess.CalledProcessError:
                        time.sleep(0.25)
                else:
                    raise RuntimeError("Disposable PostgreSQL did not start")
                sql(
                    "CREATE ROLE sdk_owner LOGIN PASSWORD 'owner' NOSUPERUSER NOBYPASSRLS;"
                    "CREATE ROLE sdk_runtime LOGIN PASSWORD 'runtime' NOSUPERUSER NOBYPASSRLS;"
                    "CREATE DATABASE sdk OWNER sdk_owner;"
                )
                admin_uri = admin_uri.rsplit("/", 1)[0] + "/sdk"
                sql("ALTER DATABASE sdk SET search_path TO hot, public;")
                owner_uri = f"postgresql://sdk_owner:owner@127.0.0.1:{pgport}/sdk"
                runtime_uri = f"postgresql://sdk_runtime:runtime@127.0.0.1:{pgport}/sdk"
                # Extensions are infrastructure setup, not runtime privileges.
                sql(
                    "CREATE SCHEMA hot AUTHORIZATION sdk_owner;"
                    "CREATE EXTENSION vector WITH SCHEMA hot;"
                    "CREATE EXTENSION pg_trgm WITH SCHEMA hot;"
                )
                init_uri = owner_uri
            else:
                init_uri = f"sqlite://{work / 'hot.db'}"
                runtime_uri = init_uri

            with (work / "init.log").open("w") as log:
                result = subprocess.run(
                    [
                        args.hot,
                        "init",
                        str(work / "app"),
                        "--db.uri",
                        init_uri,
                        "--db.migration-uri",
                        init_uri,
                    ],
                    env=dict(env, HOT_DB_RLS_ENFORCE="false")
                    if args.backend == "postgres"
                    else env,
                    stdout=log,
                    stderr=log,
                    check=False,
                )

            if result.returncode:
                raise RuntimeError((work / "init.log").read_text()[-4000:])

            if args.backend == "postgres":
                subprocess.run(
                    [
                        psql,
                        owner_uri,
                        "-X",
                        "-v",
                        "runtime_role=sdk_runtime",
                        "-v",
                        "schema=hot",
                        "-f",
                        str(args.hot_repo / "resources/db/postgres/provision_runtime.sql"),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                )
                assert (
                    sql(
                        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user",
                        runtime_uri,
                    )
                    == "f"
                )
                assert (
                    sql(
                        "SELECT count(*) FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner "
                        "WHERE r.rolname='sdk_runtime'",
                        runtime_uri,
                    )
                    == "0"
                )
                root_id, user_id, env_id = sql(
                    'SELECT o.org_id,u.user_id,e.env_id FROM hot.org o CROSS JOIN hot."user" u '
                    "JOIN hot.env e ON true WHERE e.org_id=o.org_id LIMIT 1"
                ).split("|")
                sql(f"UPDATE hot.org SET org_type='organization' WHERE org_id='{root_id}'")
            else:
                db = sqlite3.connect(work / "hot.db")
                root_bytes, user_bytes, env_bytes = db.execute(
                    "SELECT o.org_id,u.user_id,e.env_id FROM org o CROSS JOIN user u "
                    "JOIN env e ON e.org_id=o.org_id LIMIT 1"
                ).fetchone()
                root_id, user_id, env_id = map(
                    lambda b: str(uuid.UUID(bytes=b)), (root_bytes, user_bytes, env_bytes)
                )
                db.execute("UPDATE org SET org_type='organization' WHERE org_id=?", (root_bytes,))
                db.commit()

            def manager(name):
                identity, secret = uuid.uuid4(), secrets.token_hex(16)
                digest = hashlib.sha256(secret.encode()).digest()
                capabilities = json.dumps(
                    [
                        "children:read",
                        "children:create",
                        "children:update",
                        "environments:create",
                        "runtime-keys:create",
                        "usage:read",
                    ]
                )
                if args.backend == "postgres":
                    sql(
                        "INSERT INTO hot.management_key (management_key_id,root_org_id,"
                        "created_by_user_id,name,secret_hash,capabilities,all_children,"
                        "delegation_permissions,expires_at) VALUES "
                        f"('{identity}','{root_id}','{user_id}','{name}',"
                        f"decode('{digest.hex()}','hex'),'{capabilities}',true,"
                        "'{\"*:*\":[\"*\"]}',now()+interval '1 hour')"
                    )
                else:
                    db.execute(
                        "INSERT INTO management_key (management_key_id,root_org_id,"
                        "created_by_user_id,name,secret_hash,capabilities,all_children,"
                        "delegation_permissions,expires_at) VALUES (?,?,?,?,?,?,1,?,"
                        "strftime('%Y-%m-%dT%H:%M:%fZ','now','+1 hour'))",
                        (
                            identity.bytes,
                            root_bytes,
                            user_bytes,
                            name,
                            digest,
                            capabilities,
                            '{"*:*": ["*"]}',
                        ),
                    )
                    db.commit()
                return str(identity), f"hm_{identity.hex}_{secret}"

            management_id, management_token = manager("sdk-management")
            # Root runtime key is intentionally independent of the management token.
            key_id, secret = uuid.uuid4(), secrets.token_hex(32)
            key_data = json.dumps(
                {
                    "algorithm": "sha256",
                    "hash": base64.b64encode(hashlib.sha256(secret.encode()).digest()).decode(),
                }
            )
            if args.backend == "postgres":
                sql(
                    "INSERT INTO hot.api_key(api_key_id,env_id,description,key_data,"
                    "created_by_user_id,permissions) VALUES "
                    f"('{key_id}','{env_id}','sdk-root','{key_data}','{user_id}',"
                    '\'{"*:*":["*"]}\')'
                )
            else:
                db.execute(
                    "INSERT INTO api_key(api_key_id,env_id,description,key_data,"
                    "created_by_user_id,permissions) VALUES (?,?,?,?,?,?)",
                    (key_id.bytes, env_bytes, "sdk-root", key_data, user_bytes, '{"*:*": ["*"]}'),
                )
                db.commit()
            root_token = f"hot_{key_id.hex}_{secret}"
            api_env = env.copy()
            api_env["HOT_DB_URI"] = runtime_uri
            api_env["HOT_DB_RLS_ENFORCE"] = "true"
            # Redis enables real SSE ingress for the PostgreSQL fixture.
            redis_args = []
            if args.backend == "postgres":
                redis_name = name + "-redis"
                containers.append(redis_name)
                subprocess.run(
                    [
                        "docker",
                        "run",
                        "-d",
                        "--name",
                        redis_name,
                        "-p",
                        "127.0.0.1::6379",
                        "valkey/valkey:8-alpine",
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
                bound = subprocess.check_output(
                    ["docker", "port", redis_name, "6379/tcp"], text=True
                )
                redis_args = ["--redis.uri", "redis://127.0.0.1:" + bound.strip().rsplit(":", 1)[1]]
            with (work / "api.log").open("w") as log:
                server = subprocess.Popen(
                    [
                        args.hot,
                        "api",
                        "--db.uri",
                        runtime_uri,
                        "--api.host",
                        "127.0.0.1",
                        "--api.port",
                        str(port),
                        "--queue.type",
                        "sqlite",
                        *redis_args,
                    ],
                    cwd=work / "app",
                    env=api_env,
                    stdout=log,
                    stderr=log,
                )

            def request(method, path, token, body=None):
                data = None if body is None else json.dumps(body).encode()
                req = urllib.request.Request(
                    base_url + path,
                    data=data,
                    method=method,
                    headers={
                        "Authorization": "Bearer " + token,
                        "Content-Type": "application/json",
                    },
                )
                try:
                    response = urllib.request.urlopen(req, timeout=10)
                except urllib.error.HTTPError as error:
                    raise RuntimeError(
                        f"{method} {path}: {error.code} {error.read().decode()}"
                    ) from None
                with response:
                    raw = response.read()
                    return json.loads(raw)["data"] if raw else None

            for _ in range(120):
                try:
                    request("GET", "/v1/env", root_token)
                    break
                except (OSError, RuntimeError):
                    if server.poll() is not None:
                        raise RuntimeError(
                            "Fixture API exited; " + (work / "api.log").read_text()[-4000:]
                        ) from None
                    time.sleep(0.25)
            else:
                raise RuntimeError("Fixture API did not become ready")
            # Check the published contract, not just a frozen SDK coverage table.
            with urllib.request.urlopen(base_url + "/openapi.json", timeout=10) as response:
                spec = json.load(response)
            actual = {
                (method.upper(), path)
                for path, item in spec["paths"].items()
                for method in ("get", "post", "put", "patch", "delete")
                if method in item
            }
            repo = Path(__file__).resolve().parents[1]
            candidates = [
                "tests/openapi_operations.json",
                "packages/sdk/test/fixtures/openapi-operations.json",
                "testdata/openapi-operations.json",
                "tests/fixtures/openapi-operations.json",
                "src/test/resources/openapi-operations.json",
            ]
            operations_path = next(repo / name for name in candidates if (repo / name).exists())
            expected = {
                (op["method"].upper(), op["path"]) for op in json.loads(operations_path.read_text())
            }
            if actual != expected:
                raise RuntimeError(
                    f"SDK OpenAPI fixture differs from server: missing={actual - expected}; "
                    f"removed={expected - actual}"
                )
            print(f"Verified {len(actual)} live OpenAPI operations", flush=True)
            fixture = {
                "root_org_id": root_id,
                "root_token": root_token,
                "management_token": management_token,
            }
            # Prepare previously issued credentials, then revoke/narrow their manager.
            # SDK tests verify the effective chain rather than merely presenting bad tokens.
            for state in ("revoked", "narrowed"):
                issuer_id, issuer_token = manager("sdk-" + state)
                child = request(
                    "POST",
                    "/v1/management/children",
                    management_token,
                    {
                        "name": state,
                        "slug": "sdk-" + uuid.uuid4().hex,
                        "idempotency_key": uuid.uuid4().hex,
                    },
                )
                child_id, child_env = child["org"]["org_id"], child["env_id"]
                key = request(
                    "POST",
                    f"/v1/management/children/{child_id}/environments/{child_env}/api-keys",
                    issuer_token,
                    {"description": state, "permissions": {"*:*": ["*"]}},
                )
                service = request(
                    "POST",
                    "/v1/service-keys",
                    key["token"],
                    {
                        "name": state,
                        "permissions": {"event:*": ["create", "read"], "stream:*": ["read"]},
                    },
                )
                session = request(
                    "POST", "/v1/sessions", key["token"], {"permissions": {"*:*": ["*"]}}
                )
                fixture[state] = [key["token"], service["token"], session["token"]]
                if args.backend == "postgres":
                    assignment = (
                        "revoked_at=now()"
                        if state == "revoked"
                        else 'delegation_permissions=\'{"event:sdk:*":["create","read"],'
                        '"stream:*":["read"]}\''
                    )
                    sql(
                        f"UPDATE hot.management_key SET {assignment} "
                        f"WHERE management_key_id='{issuer_id}'"
                    )
                else:
                    assignment = (
                        "revoked_at=CURRENT_TIMESTAMP"
                        if state == "revoked"
                        else 'delegation_permissions=\'{"event:sdk:*":["create","read"],'
                        '"stream:*":["read"]}\''
                    )
                    db.execute(
                        f"UPDATE management_key SET {assignment} WHERE management_key_id=?",
                        (uuid.UUID(issuer_id).bytes,),
                    )
                    db.commit()
            if args.backend == "sqlite":
                db.close()
            fixture_path = work / "credentials.json"
            fixture_path.write_text(json.dumps(fixture))
            fixture_path.chmod(0o600)
            test_env = env.copy()
            test_env.update(HOT_TEST_BASE_URL=base_url, HOT_TEST_SUBORG_FIXTURE=str(fixture_path))
            print(f"Running sub-org integration against {args.backend}", flush=True)
            result = subprocess.run(command, env=test_env)
            if result.returncode:
                print((work / "api.log").read_text()[-4000:])
            raise SystemExit(result.returncode)
        finally:
            if server:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
            for container in reversed(containers):
                subprocess.run(
                    ["docker", "rm", "-fv", container],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )


if __name__ == "__main__":
    main()
