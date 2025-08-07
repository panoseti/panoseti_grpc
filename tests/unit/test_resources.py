import pytest
from pathlib import Path
from daq_data.resources import _parse_dp_name, _parse_seqno, get_dp_name_from_props
from daq_data.daq_data_pb2 import PanoImage


def test_parse_dp_name():
    """Test extraction of data product name from filename."""
    fname = "start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_0.pff"
    assert _parse_dp_name(fname) == "img16"

    fname_ph = "start_2024-07-25T04_34_46Z.dp_ph256.bpp_2.module_3.seqno_0.pff"
    assert _parse_dp_name(fname_ph) == "ph256"

    with pytest.raises(ValueError):
        _parse_dp_name("invalid_filename.txt")


def test_parse_seqno():
    """Test extraction of sequence number from filename."""
    fname = "start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.seqno_123.pff"
    assert _parse_seqno(fname) == 123

    fname_no_seqno = "start_2024-07-25T04_34_46Z.dp_img16.bpp_2.module_1.pff"
    assert _parse_seqno(fname_no_seqno) == 0


def test_get_dp_name_from_props():
    """Test derivation of data product name from image properties."""
    # img16
    assert get_dp_name_from_props(PanoImage.Type.MOVIE, [32, 32], 2) == 'img16'
    # ph1024
    assert get_dp_name_from_props(PanoImage.Type.PULSE_HEIGHT, [32, 32], 2) == 'ph1024'
    # ph256
    assert get_dp_name_from_props(PanoImage.Type.PULSE_HEIGHT, [16, 16], 2) == 'ph256'

    with pytest.raises(ValueError):
        get_dp_name_from_props(PanoImage.Type.MOVIE, [16, 16], 2)  # Invalid combo
