import pytest

from src.common.config import Settings, env_bool, safe_child, safe_table_list


def test_safe_child_rejects_path_escape():
    with pytest.raises(ValueError):
        safe_child("data/dev/bronze", "../gold")
    with pytest.raises(ValueError):
        safe_child("data/dev/bronze", "/tmp/escape")


def test_safe_table_list_accepts_only_identifiers():
    assert safe_table_list("trips, payments,zones") == ["trips", "payments", "zones"]
    with pytest.raises(ValueError):
        safe_table_list("trips,../../tmp")


def test_boolean_configuration_is_strict(monkeypatch):
    monkeypatch.setenv("FLAG", "yes")
    assert env_bool("FLAG") is True
    monkeypatch.setenv("FLAG", "sometimes")
    with pytest.raises(ValueError):
        env_bool("FLAG")


def test_settings_validate_environment_and_database(monkeypatch):
    monkeypatch.setenv("ENV", "dev/test")
    with pytest.raises(ValueError):
        Settings.from_env()
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    with pytest.raises(ValueError):
        Settings.from_env(require_database=True)
