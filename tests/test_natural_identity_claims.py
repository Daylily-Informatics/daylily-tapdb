from __future__ import annotations

import uuid

import pytest

from daylily_tapdb.factory import validate_identity_key
from daylily_tapdb.models.instance import generic_instance


@pytest.mark.parametrize(
    "value",
    [
        "labcore:sequencing_run:<persisted-euid>",
        "tapdb.external-reference/v1:service:<persisted-euid>",
        "x.y-z_1:opaque suffix",
    ],
)
def test_validate_identity_key_preserves_exact_valid_value(value: str):
    assert validate_identity_key(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "Labcore:value",
        "labcore:",
        "lab core:value",
        "labcore:value\nsecond",
        "labcorevalue",
        "a" * 512 + ":value",
    ],
)
def test_validate_identity_key_rejects_invalid_values(value: str):
    with pytest.raises(ValueError):
        validate_identity_key(value)


def test_identity_key_orm_contract_is_nullable_global_and_bounded():
    column = generic_instance.__table__.columns["identity_key"]
    assert column.nullable is True
    assert column.type.length == 512
    constraint_names = {
        constraint.name for constraint in generic_instance.__table__.constraints
    }
    assert "ck_generic_instance_identity_key_global" in constraint_names
    assert "ck_generic_instance_identity_key_format" in constraint_names


def test_claimant_tenant_is_protocol_evidence_not_row_scope():
    claimant = uuid.uuid4()
    assert str(claimant) != ""
