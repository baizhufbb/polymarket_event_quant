import sys

import pytest

from polymarket_bot import cli


class _Stop(Exception):
    """Raised to leave main() as soon as the setting under test has run."""


def test_main_shortens_the_thread_handoff(monkeypatch) -> None:
    """A 25 ms cadence is finer than the interpreter's default 5 ms hand-off.

    Left at the default, a loop whose slot has come waits behind whatever
    else holds the interpreter; on a single core every member waits together
    and the offsets between them vanish.
    """
    recorded: list[float] = []
    monkeypatch.setattr(sys, "setswitchinterval", recorded.append)
    monkeypatch.setattr(sys, "argv", ["bot.py", "paper-status"])

    def stop(*args, **kwargs):
        raise _Stop

    monkeypatch.setattr(cli, "PaperDatabase", stop)

    with pytest.raises(_Stop):
        cli.main()

    assert recorded == [cli.THREAD_SWITCH_SECONDS]
    # Measured on the server: the 5 ms default missed 188 of 400 slots and
    # 4 ms missed 169, while 1 ms and below missed none. Anything coarser than
    # a millisecond puts the defect back, so a millisecond is the bar - not
    # merely "finer than the default".
    assert cli.THREAD_SWITCH_SECONDS <= 0.001
