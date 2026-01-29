import pytest
from pathlib import Path
from daq_data.resources import _parse_dp_name, _parse_seqno, get_dp_name_from_props, parse_pano_image
from daq_data.daq_data_pb2 import PanoImage
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import ParseDict
import numpy as np


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


def test_parse_pano_image():
    """Test the unpacking and type conversion of a PanoImage message."""
    header_dict = {
        "quabo_0": {"pkt_tai": 529, "pkt_nsec": 779007488, "tv_sec": 1721882092, "tv_usec": 779336}
    }
    raw_image = PanoImage(
        type=PanoImage.Type.MOVIE,
        header=ParseDict(header_dict, Struct()),
        image_array=[i for i in range(1024)],  # 32x32
        shape=[32, 32],
        bytes_per_pixel=2,
        module_id=42
    )

    parsed = parse_pano_image(raw_image)

    assert isinstance(parsed, dict)
    assert parsed['type'] == 'MOVIE'
    assert parsed['module_id'] == 42
    assert isinstance(parsed['image_array'], np.ndarray)
    assert parsed['image_array'].shape == (32, 32)
    assert parsed['image_array'].dtype == np.uint16
    assert 'pandas_unix_timestamp' in parsed['header']


def test_parse_dp_name_with_malformed_input():
    """Test extraction of data product name from invalid filenames."""
    with pytest.raises(ValueError, match="Could not parse data product name"):
        _parse_dp_name("a_filename_without_dp_field.pff")
    with pytest.raises(ValueError, match="Could not parse data product name"):
        _parse_dp_name("invalid.txt")


def test_parse_seqno_with_malformed_input():
    """Test extraction of sequence number from a filename without one."""
    # A filename without a seqno field should default to 0
    assert _parse_seqno("start.dp_img16.module_1.pff") == 0


def test_get_dp_name_from_props_with_invalid_combinations():
    """Test derivation of data product name with invalid property combinations."""
    # Test with an unsupported shape
    with pytest.raises(ValueError, match="Unknown data product"):
        get_dp_name_from_props(PanoImage.Type.MOVIE, [24, 24], 2)

    # Test with an unsupported bytes-per-pixel value
    with pytest.raises(ValueError, match="Unknown data product"):
        get_dp_name_from_props(PanoImage.Type.MOVIE, [32, 32], 4)

    # Test pulse-height with 1 byte-per-pixel (invalid)
    with pytest.raises(ValueError, match="Unknown data product"):
        get_dp_name_from_props(PanoImage.Type.PULSE_HEIGHT, [32, 32], 1)