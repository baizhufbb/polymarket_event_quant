from polymarket_bot.conditional_tokens import binary_token_ids


def test_derives_live_clob_binary_token_ids() -> None:
    condition_id = (
        "0xbc2143c70ad2af9481e8dd46eb538f267d7c23ad781ddf5380e9a00d46e9e9cd"
    )

    assert binary_token_ids(condition_id) == (
        "79690064268849976430077014249758289461450439393597627695915949752735045024504",
        "62179804371407207770226633627836131020602362404226320415203031505393883439742",
    )
