from __future__ import annotations

from eth_utils import keccak


_FIELD_MODULUS = (
    21888242871839275222246405745257275088696311157297823662689037894645226208583
)
_CURVE_B = 3
_ODD_COLLECTION_FLAG = 1 << 254
_CTF_COLLATERAL = bytes.fromhex("2791Bca1f2de4661ED88A30C99A7a9449Aa84174")


def _collection_id(condition_id: bytes, index_set: int) -> bytes:
    seed = int.from_bytes(
        keccak(condition_id + index_set.to_bytes(32, "big")), "big"
    )
    odd = bool(seed >> 255)
    x = seed

    while True:
        x = (x + 1) % _FIELD_MODULUS
        yy = (pow(x, 3, _FIELD_MODULUS) + _CURVE_B) % _FIELD_MODULUS
        y = pow(yy, (_FIELD_MODULUS + 1) // 4, _FIELD_MODULUS)
        if pow(y, 2, _FIELD_MODULUS) == yy:
            break

    if (y % 2 == 0) == odd:
        y = _FIELD_MODULUS - y
    if y % 2 == 1:
        x ^= _ODD_COLLECTION_FLAG
    return x.to_bytes(32, "big")


def binary_token_ids(condition_id: str) -> tuple[str, str]:
    """Derive the Up/Down CLOB asset IDs for a binary CTF condition."""
    raw_condition = bytes.fromhex(condition_id.removeprefix("0x"))
    if len(raw_condition) != 32:
        raise ValueError("condition id must contain 32 bytes")

    token_ids = []
    for index_set in (1, 2):
        collection_id = _collection_id(raw_condition, index_set)
        token_ids.append(
            str(int.from_bytes(keccak(_CTF_COLLATERAL + collection_id), "big"))
        )
    return token_ids[0], token_ids[1]
