from __future__ import annotations

import pytest

from daylily_tapdb.advisory_locks import (
    advisory_lock_fingerprint,
    derive_advisory_lock_key,
)


def test_framed_advisory_lock_derivation_is_deterministic_signed_int64():
    first = derive_advisory_lock_key("tapdb.test", "ab", "c")
    replay = derive_advisory_lock_key("tapdb.test", "ab", "c")
    differently_framed = derive_advisory_lock_key("tapdb.test", "a", "bc")
    assert first == replay
    assert first != differently_framed
    assert -(2**63) <= first < 2**63


def test_advisory_receipt_fingerprint_redacts_inputs():
    fingerprint = advisory_lock_fingerprint("tapdb.test", "sensitive-object")
    assert len(fingerprint) == 64
    assert "sensitive" not in fingerprint


@pytest.mark.parametrize("value", ["", None])
def test_advisory_derivation_rejects_empty_frames(value):
    with pytest.raises(ValueError):
        derive_advisory_lock_key("tapdb.test", value)
