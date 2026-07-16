#!/usr/bin/env python3
"""Mint a full-access API key directly in a local `hot dev` sqlite database.

Usage: mint_key.py <path-to-.hot/db/hot.sqlite.db>
Prints the plaintext key (hot_<uuid>_<secret>) on stdout.
"""
import base64
import hashlib
import json
import secrets
import sqlite3
import sys
import uuid

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
env_id = conn.execute("select env_id from env limit 1").fetchone()[0]
user_id = conn.execute("select user_id from user limit 1").fetchone()[0]

key_id = uuid.uuid4()
secret = secrets.token_hex(32)
key_data = json.dumps(
    {
        "algorithm": "sha256",
        "hash": base64.b64encode(hashlib.sha256(secret.encode()).digest()).decode(),
    }
)

conn.execute(
    "insert into api_key (api_key_id, env_id, description, key_data,"
    " created_by_user_id, permissions) values (?, ?, ?, ?, ?, ?)",
    (
        key_id.bytes,
        env_id,
        "sdk-integration-tests",
        key_data,
        user_id,
        json.dumps({"*:*": ["*"]}),
    ),
)
conn.commit()
print(f"hot_{key_id.hex}_{secret}")
