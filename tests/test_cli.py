import argparse

import pytest

from polymarket_bot import cli


def test_a_cadence_below_a_millisecond_is_rejected() -> None:
    """Three orders of magnitude past what the venue accepts from one account."""
    for good in ("1", "20", "25", "250"):
        assert cli._placement_interval_ms_arg(good) > 0
    for bad in ("0", "0.5", "0.001", "0.0001"):
        with pytest.raises(argparse.ArgumentTypeError):
            cli._placement_interval_ms_arg(bad)


def test_the_noise_filter_covers_both_chatty_loggers() -> None:
    """Expected knocking replies are routine traffic, fully recorded in
    attempts.jsonl; the human log must not drown in them - run25 wrote three
    hundred thousand blank-reason lines in one night. Real errors must still
    pass, and a timeout is one now that requests are no longer cancelled on
    a timer."""
    import logging

    from polymarket_bot.cli import _ExpectedOrderEngineFilter

    noise_filter = _ExpectedOrderEngineFilter()

    def record(name, message):
        return logging.LogRecord(name, logging.ERROR, "", 0, message, (), None)

    submitter = "polymarket_bot.knocker"
    helpers = "py_clob_client_v2.http_helpers.helpers"

    # routine traffic: silenced
    assert not noise_filter.filter(
        record(submitter, 'request error status=400 body={"error":"invalid token id"}')
    )
    assert not noise_filter.filter(record(helpers, "... market not found ..."))

    # real trouble: passes
    assert noise_filter.filter(
        record(submitter, "[knocker] request error: ConnectError")
    )
    assert noise_filter.filter(
        record(submitter, "[knocker] request error: context deadline exceeded")
    )
    assert noise_filter.filter(record(helpers, "request error status=500 boom"))

    # unrelated loggers: untouched
    assert noise_filter.filter(record("polymarket_bot", "market not found"))
