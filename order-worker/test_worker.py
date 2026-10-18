"""Unit tests for the worker's pure logic. No database, no AWS, no network needed."""
import pytest

from worker import is_poison, parse_order_id, status_after_failure


def test_faulty_items_are_poison():
    assert is_poison("FAULTY-Widget")
    assert is_poison("faulty-widget")          # case does not matter
    assert is_poison("Super FAULTY Cable")     # anywhere in the name


def test_normal_items_are_not_poison():
    assert not is_poison("Notebook")
    assert not is_poison("USB-C Cable")


def test_custom_keyword():
    assert is_poison("Broken Lamp", keyword="broken")


def test_failure_before_limit_is_retrying():
    assert status_after_failure(1, 3) == "RETRYING"
    assert status_after_failure(2, 3) == "RETRYING"


def test_failure_at_limit_is_failed():
    assert status_after_failure(3, 3) == "FAILED"
    assert status_after_failure(4, 3) == "FAILED"


def test_parse_order_id_valid():
    assert parse_order_id('{"order_id": 42}') == 42
    assert parse_order_id('{"order_id": "7"}') == 7


@pytest.mark.parametrize("body", ["not json", "{}", '{"order_id": "abc"}', "null", ""])
def test_parse_order_id_rejects_bad_bodies(body):
    with pytest.raises(ValueError):
        parse_order_id(body)
