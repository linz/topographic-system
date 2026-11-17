from .kart_import_theme import _commit_message, _schema_changed_note


def test_commit_message_has_no_body_without_notes():
    assert _commit_message("feat(airport): import release 67") == "feat(airport): import release 67"


def test_commit_message_appends_each_note_as_its_own_paragraph():
    message = _commit_message("feat(airport): import release 67", _schema_changed_note("v0.4"))
    assert message == ("feat(airport): import release 67\n\nschema changed to v0.4")


def test_schema_changed_note():
    assert _schema_changed_note("v0.4") == "schema changed to v0.4"
