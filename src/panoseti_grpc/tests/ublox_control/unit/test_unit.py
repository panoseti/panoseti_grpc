# tests/test_unit.py

import pytest
from ublox_control.resources import get_f9t_redis_key, ubx_to_dict
from ublox_control.initialize.conf_gnss import _layers_mask, _split_scaled_llh
from pyubx2 import UBXMessage


def test_get_f9t_redis_key_valid():
    """Tests Redis key generation with a valid UID."""
    key = get_f9t_redis_key("ZED-F9T", "DF03A241BC", "TIM-TP")
    assert key == "UBLOX_ZED-F9T_DF03A241BC_TIM-TP"


def test_get_f9t_redis_key_invalid():
    """Tests that an invalid UID raises a ValueError."""
    with pytest.raises(ValueError):
        get_f9t_redis_key("ZED-F9T", "INVALID", "TIM-TP")
    with pytest.raises(ValueError):
        get_f9t_redis_key("ZED-F9T", "12345", "TIM-TP")


def test_layers_mask():
    """Tests the layer mask generation function."""
    assert _layers_mask(["RAM"]) == 0x01
    assert _layers_mask(["BBR"]) == 0x02
    assert _layers_mask(["FLASH"]) == 0x04
    assert _layers_mask(["RAM", "FLASH"]) == 0x05
    with pytest.raises(ValueError):
        _layers_mask(["INVALID_LAYER"])


def test_split_scaled_llh():
    """Tests the conversion of LLH coordinates to u-blox format."""
    lat_i, lat_hp, lon_i, lon_hp, h_cm, h_hp = _split_scaled_llh(37.4219999, -122.0840575, 12.345)
    assert lat_i == 374219999
    assert lat_hp == 0
    assert lon_i == -1220840575
    assert lon_hp == 0
    assert h_cm == 1234
    assert h_hp == 50


def test_ubx_to_dict():
    """Tests the conversion of a UBXMessage to a dictionary."""
    # Create a sample UBXMessage (ACK-ACK)
    msg = UBXMessage('ACK', 'ACK-ACK', 0, clsID=0x05, msgID=0x01)
    msg_dict = ubx_to_dict(msg)

    assert "identity" in msg_dict
    assert msg_dict["identity"] == "ACK-ACK"
    assert "clsID" in msg_dict
    assert msg_dict["clsID"] == 5
    # Internal attributes starting with '_' should not be present
    assert "_prot_id" not in msg_dict
    assert "_payload" not in msg_dict
