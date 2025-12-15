from kart.kart_commands import get_kart_command


def test_check_kart_command() -> None:
    """
    tests that the kart command is built correctly
    """
    kart_command = get_kart_command(
        "fetch", ["--option1", "value1", "--option2", "value2"]
    )
    assert kart_command == ["fetch", "--option1", "value1", "--option2", "value2"]


def test_check_kart_command_invalid() -> None:
    """
    tests that the kart command raises an error when an invalid command is requested
    """
    try:
        get_kart_command("invalid_command", ["--option1", "value1"])
    except ValueError as e:
        assert str(e) == "Unsupported kart command: invalid_command"
    else:
        assert False, "ValueError was not raised for an unsupported kart command"
