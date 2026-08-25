from __future__ import annotations

import re
import time
from collections.abc import Mapping
from typing import Any, TypedDict

from openpine_contracts import validate_payload
from openpine_contracts.hashing import (
    CONTENT_HASH_ALG,
    SERIALIZER_ID,
    seal_content_hash,
    verify_content_hash,
)

from marketdata_provider.errors import MDValidationError

PRODUCER = "marketdata-provider"
PRODUCER_VERSION = "5.0.0-rc.5"
SCHEMA_VERSION = "2.1.0"
BAR_SCHEMA_ID = "openpine.marketdata.bar.v2"
SNAPSHOT_SCHEMA_ID = "openpine.marketdata.v2"
_COMMIT_RE = re.compile(r"^(?!0{40}$)[0-9a-f]{40}$")
_STACK_RE = re.compile(r"^sha256:(?!0{64}$)[0-9a-f]{64}$")


class ProviderRevision(TypedDict):
    known: bool
    revision: str | None


def utc_now_ms() -> int:
    return time.time_ns() // 1_000_000


def known_provider_revision(revision: object) -> ProviderRevision:
    if not isinstance(revision, str) or not revision:
        raise MDValidationError("provider_revision revision is required")
    return {"known": True, "revision": revision}


def normalize_provider_revision(value: object) -> ProviderRevision:
    if not isinstance(value, Mapping):
        raise MDValidationError("provider_revision must be a typed identity")
    if set(value) != {"known", "revision"}:
        raise MDValidationError("provider_revision must contain known and revision")
    if value.get("known") is not True:
        raise MDValidationError("provider_revision must be known in production")
    return known_provider_revision(value.get("revision"))


def validate_artifact_identity(
    *, producer_commit: object, stack_id: object
) -> tuple[str, str]:
    if (
        not isinstance(producer_commit, str)
        or _COMMIT_RE.fullmatch(producer_commit) is None
    ):
        raise MDValidationError(
            "producer_commit must be an exact nonzero 40-hex commit"
        )
    if not isinstance(stack_id, str) or _STACK_RE.fullmatch(stack_id) is None:
        raise MDValidationError(
            "stack_id must be an exact nonzero sha256 manifest hash"
        )
    return producer_commit, stack_id


def envelope_metadata(
    *, producer_commit: object, stack_id: object, created_at_utc_ms: object
) -> dict[str, Any]:
    commit, stack = validate_artifact_identity(
        producer_commit=producer_commit, stack_id=stack_id
    )
    if (
        isinstance(created_at_utc_ms, bool)
        or not isinstance(created_at_utc_ms, int)
        or created_at_utc_ms < 0
    ):
        raise MDValidationError("created_at_utc_ms must be a nonnegative integer")
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER,
        "producer_version": PRODUCER_VERSION,
        "producer_commit": commit,
        "stack_id": stack,
        "created_at_utc_ms": created_at_utc_ms,
        "serializer_id": SERIALIZER_ID,
        "content_hash_alg": CONTENT_HASH_ALG,
    }


def seal_and_validate(schema_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    sealed = seal_content_hash(payload, schema_id=schema_id)
    validate_payload(schema_id, sealed)
    if not verify_content_hash(sealed, schema_id=schema_id):
        raise MDValidationError("content_hash verification failed after sealing")
    return sealed
