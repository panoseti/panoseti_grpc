#!/usr/bin/env python3
"""
Filename: conf_gnss.py
Author: Ben G. + ChatGPT
Date: August 2025
Version: 1.0
Description: Script to configure ZED F9P/T receivers from a JSON file. Heavy help from ChatGPT.
License: MIT License
"""

import argparse, csv, inspect, pyubx2, re, serial, sys, time
import logging
import json5 as json
from typing import List, Tuple
from pyubx2 import (
    UBXMessage,
    UBXReader,
    UBXStreamError,
    UBXMessageError,
    UBXTypeError,
    UBXParseError,
    POLL
)

try:
    from pyubx2 import UBX_CONFIG_DATABASE
    DTYPE_BY_ID = {kid: dtype for (_name, (kid, dtype)) in UBX_CONFIG_DATABASE.items()}
except Exception:
    DTYPE_BY_ID = {}

from pyubx2.ubxhelpers import cfgname2key, process_monver  # pretty parser for MON-VER
from panoseti_grpc.ublox_control.resources import make_rich_logger

confg_gnss_logger = make_rich_logger("confg_gnss")

UBX_EXC = (UBXStreamError, UBXMessageError, UBXTypeError, UBXParseError)

# Layer bitmasks for CFG-VALSET (set) and layer selectors for CFG-VALGET (poll)
SET_LAYER = {"RAM": 0x01, "BBR": 0x02, "FLASH": 0x04}
POLL_LAYER = {"RAM": 0, "BBR": 1, "FLASH": 2, "DEFAULT": 7}
LAYER_ORDER = ["RAM", "BBR", "FLASH", "DEFAULT"]  # FLASH may NAK VALGET; ok if it does
_HEXKEY_RE = re.compile(r"^CFG_0x([0-9A-Fa-f]{8})$")

def _to_float(x):
    """
    Helper function to see if a value is a number
    """
    try:
        return float(x)
    except Exception:
        return None

def _fmt_val(v, dtype):
    """
    Helper function for printing out types/units in register descriptions
    """
    # dtype examples: 'U1','U2','U4','I4','R8','L','E1'
    #bytes -> hex
    
    if isinstance(v, (bytes, bytearray)):
        return v.hex()

    # if dtype is known, prefer it
    if dtype:
        d = dtype.upper()
        if d == "L":                      # logical/bool
            return "1" if bool(v) else "0"
        if d.startswith("R"):             # float types R4/R8
            try:
                return f"{float(v):.6g}"
            except Exception:
                return str(v)

    # fallback (no dtype): best-effort pretty
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)

def _layers_mask(names: List[str]) -> int:
    """
    Set up the mask for which layers you want to write to
    """
    mask = 0
    for n in names:
        if n.upper() not in SET_LAYER:
            raise ValueError(f"Unknown layer '{n}'. Use RAM, BBR, FLASH.")
        mask |= SET_LAYER[n.upper()]
    return mask

def _merge_cfg_kvmap(dst: dict, kvmap: dict) -> None:
    """
    Not used anymore. Was used when trying to figure out format of I/O
    """
    for k, v in kvmap.items():
        _merge_cfg_one(dst, k, v)

def _log_plan(cfg_items):
    """
    Prints out registers / info about them if you set verbose mode on
    """
    confg_gnss_logger.debug("Planned writes:")
    for it in cfg_items:
        confg_gnss_logger.debug(f"  {it['name']:36s} id={hex(it['id'])} dtype={it['dtype']:>2s} value={_fmt_val(it['value'], it['dtype'])}")

def _await_ack(ser, timeout=3.0) -> bool:
    """
    Wait for UBX-ACK-ACK or UBX-ACK-NAK after a command.
    """
    rdr = UBXReader(ser, protfilter=2)  # UBX only
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            (raw, parsed) = rdr.read()
        except UBX_EXC:
            continue
        if parsed and parsed.identity in ("ACK-ACK", "ACK-NAK"):
            return parsed.identity == "ACK-ACK"
    return False

def _norm_name(s: str) -> str:
    """
    Standardize how regisers are written (uppercase with underscores)
    """
    return s.strip().replace("-", "_").upper()

def _fmt_eng(value, dtype, scale_str, unit_str):
    """
    Return (eng_value_str, unit_str_shown). If scale is numeric, eng = raw * scale.
    If scale is '-' or not numeric, we just append the unit (if any).
    """
    # bytes / non-numeric -> just show raw
    if isinstance(value, (bytes, bytearray)):
        return value.hex(), ""

    # bool (logical)
    if dtype == "L":
        return ("1" if bool(value) else "0"), ""

    # numeric?
    vnum = _to_float(value)
    if vnum is None:
        return str(value), ""

    scale = _to_float(scale_str)  # CSV has things like '0.000001' or '-'
    unit = (unit_str or "").strip()
    if scale is not None:
        eng = vnum * scale
        # pretty with ~6 sig figs, but keep integers clean
        s = f"{eng:.6g}"
        # if unit is '%', don't insert space; otherwise do
        u = "%" if unit == "%" else (f" {unit}" if unit and unit != "-" else "")
        return s, u
    else:
        # no scale; just append unit if present
        u = "%" if unit == "%" else (f" {unit}" if unit and unit != "-" else "")
        return f"{vnum:g}", u

def _split_scaled_llh(lat_deg, lon_deg, h_m):
    """
    Convert latitude, longitude, and height into F9T units (F9P finds its own)
    """
    # INT + HP parts: INT in 1e-7 deg (lat/lon), cm (height). HP in 1e-9 deg, 0.1 mm (height).
    def ll_to_int_hp(deg):
        tot_1e9 = int(round(deg * 1e9))
        int_1e7 = int(tot_1e9 // 100)           # truncate to 1e-7 deg
        hp_1e9  = int(tot_1e9 - int_1e7 * 100)  # residual in 1e-9 deg
        # clamp HP into signed 8-bit range if needed (typical formats are small ints)
        return int_1e7, hp_1e9

    lat_i, lat_hp = ll_to_int_hp(lat_deg)
    lon_i, lon_hp = ll_to_int_hp(lon_deg)

    tot_0p1mm = int(round(h_m * 10000))        # meters -> 0.1 mm
    h_cm      = int(tot_0p1mm // 100)          # cm
    h_hp      = int(tot_0p1mm - h_cm * 100)    # 0.1 mm residual
    return lat_i, lat_hp, lon_i, lon_hp, h_cm, h_hp


def _merge_cfg_one(dst: dict, k, v):
    """
    Take a config key and add it to the config dictionary (register key is agnostic to form)
    """
    # Accept int key IDs, hex strings, or CFG_* names (with _ or -)
    if isinstance(k, int):
        kid = k
    elif isinstance(k, str):
        s = k.strip()
        if s.lower().startswith("0x"):
            kid = int(s, 16)
        else:
            m = _HEXKEY_RE.match(s)  # e.g., CFG_0x10520007
            if m:
                kid = int(m.group(1), 16)
            else:
                try:
                    kid, _ = cfgname2key(_norm_name(s))
                except Exception:
                    return
    else:
        return
    dst[kid] = v

def _merge_cfg_results(dst: dict, parsed) -> None:
    """
    Normalize any CFG-VALGET payload into {numeric_keyid: value}.
    Handles:
      - parsed.cfgData as dict or list[(k,v)]
      - scattered attributes keyID_XX + val*/value*
      - attributes named like CFG_* (incl. CFG_0xXXXXXXXX)
    """
    cfg = getattr(parsed, "cfgData", None) or getattr(parsed, "cfgdata", None)
    if isinstance(cfg, dict):
        for k, v in cfg.items(): _merge_cfg_one(dst, k, v); return
    if isinstance(cfg, list):
        for k, v in cfg: _merge_cfg_one(dst, k, v); return

    attrs = vars(parsed)

    # 1) Direct CFG_* attributes (what your dump shows)
    for name, val in attrs.items():
        if isinstance(name, str) and name.startswith("CFG_"):
            _merge_cfg_one(dst, name, val)

    # 2) keyID/val pairs (other pyubx2 variants)
    for name, kval in attrs.items():
        m = re.match(r'^(?:keyID|key|cfgKey|keyid)_?(\d+)$', name)
        if not m: 
            continue
        idx = m.group(1)
        for vname in (
            f"val_{idx}", f"val{idx}", f"value_{idx}", f"value{idx}",
            f"valU1_{idx}", f"valU2_{idx}", f"valU4_{idx}", f"valU8_{idx}",
            f"valI1_{idx}", f"valI2_{idx}", f"valI4_{idx}", f"valI8_{idx}",
            f"valR4_{idx}", f"valR8_{idx}",
        ):
            if vname in attrs:
                _merge_cfg_one(dst, kval, attrs[vname])
                break

def _to_cfg_items(entries):
    """
    Returns a list of dicts with {'name','id','value'}.
    We send by ID for robustness, but keep name for printing.
    """
    out = []
    for e in entries:
        name = _norm_name(str(e["key"]))
        val = e["value"]
        if isinstance(val, str) and val.lower().startswith("0x"):
            val = int(val, 16)

        # Resolve key ID and data type from the name
        kid, dtype = cfgname2key(name)
        dtype_char = dtype[0]

        # pyubx2 is strict about value types. Coerce the value to the
        # type expected by the library based on the configuration key's data type.
        try:
            if dtype_char in ('L', 'U', 'I', 'E', 'X') and not isinstance(val, int):
                val = int(val)
            elif dtype_char == 'R' and not isinstance(val, float):
                val = float(val)
        except (ValueError, TypeError) as exc:
            confg_gnss_logger.warning(f"Warning: Could not coerce value for {name} to expected type {dtype}: {exc}", file=sys.stderr)

        out.append({"name": name, "id": kid, "dtype": dtype, "value": val})
    return out

def poll_mon_ver(ser, timeout=2.5):
    """
    Checking to see if there is a device
    """
    # MON (0x0A), VER (0x04), empty payload
    msg = UBXMessage(0x0A, 0x04, 0)  # mode 0 = “no payload”; fine for MON-VER poll
    ser.write(msg.serialize())
    rdr = UBXReader(ser, protfilter=2)  # UBX only
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            raw, parsed = rdr.read()
        except UBX_EXC:
            continue
        if parsed and getattr(parsed, "identity", "") == "MON-VER":
            info = {
                "swVersion": getattr(parsed, "swVersion", None),
                "hwVersion": getattr(parsed, "hwVersion", None),
                "extensions": [v for k, v in parsed.__dict__.items() if k.startswith("extension")]
            }
            confg_gnss_logger.info(f"MON-VER: {info}")
            return info
    raise RuntimeError("No MON-VER response")

def resolve_keyid(k):
    """
    Old way of converting register to a key id
    """
    if isinstance(k, int):
        return k
    if isinstance(k, str) and k.lower().startswith("0x"):
        return int(k, 16)
    # k is a name like "CFG_RATE_MEAS"
    kid, _dtype = cfgname2key(k)  # raises if unknown
    return kid

def send_cfg_valset_grouped(ser, cfg_items, layers_mask, verbose=False, sleep_after_signal=0.3):
    """
    Apply CFG items in sensible groups so the GNSS engine restarts once:
    - Group 1: CFG_SIGNAL_*  (atomic transaction; optional short pause after)
    - Group 2: everything else (single transaction)

    Returns a flat list of ACK booleans (one per chunk).
    """
    def _send_chunks(pairs, transactional=True, tag=""):
        """
        Send in chunks -- Max 64 keys per message
        """
        acks = []
        if not pairs:
            return acks
        n = len(pairs)
        for i in range(0, n, 64):
            chunk = pairs[i:i+64]
            tx = 0
            if transactional and n > 64:
                tx = 1 if i == 0 else (3 if i + 64 >= n else 2)  # start/cont/commit
            msg = UBXMessage.config_set(layers=layers_mask, transaction=tx, cfgData=chunk)
            confg_gnss_logger.debug(f"[SET {tag}] chunk {i//64+1}/{(n+63)//64} tx={tx}, {len(chunk)} keys")
            ser.write(msg.serialize())
            ok = _await_ack(ser, timeout=2.5)
            confg_gnss_logger.debug(f"[SET {tag}] ACK={'OK' if ok else 'NAK'}")
            acks.append(ok)
        return acks

    def _order_other(items):
        """
        Order how messages are sent. Configure constellation settings first followed by
        mode setting, followed by output pulse settings, followed by output protocol settings
        """
        prio = lambda n: (0 if n.startswith("CFG_TMODE_")
                        else 1 if n.startswith("CFG_TP_")
                        else 2 if n.startswith("CFG_MSGOUT_")
                        else 3)
        return sorted(items, key=lambda it: prio(it["name"]))
    
    sig   = [it for it in cfg_items if it["name"].startswith("CFG_SIGNAL_")]
    other = _order_other([it for it in cfg_items if not it["name"].startswith("CFG_SIGNAL_")])

    confg_gnss_logger.debug(f"[SET] Group CFG_SIGNAL_*: {len(sig)} keys (atomic)")

    # Send a chunk check for acknolwedgement back
    acks = _send_chunks([(it["id"], it["value"]) for it in sig], transactional=True, tag="SIGNAL")
    if sig and sleep_after_signal:
        time.sleep(sleep_after_signal)  # one short pause after GNSS reconfig

    confg_gnss_logger.debug(f"[SET] Group other CFG_*: {len(other)} keys (atomic)")

    acks += _send_chunks([(it["id"], it["value"]) for it in other], transactional=True, tag="OTHER")
    return acks

def poll_cfg(ser, key_ids, layer_name: str, position: int = 0):
    """
    Check config values to make sure things were sucessfully set
    """
    layer = POLL_LAYER[layer_name.upper()]
    results = {}
    for i in range(0, len(key_ids), 64):
        chunk = key_ids[i : i + 64]
        poll = UBXMessage.config_poll(layer=layer, position=position, keys=chunk)
        ser.write(poll.serialize())
        rdr = UBXReader(ser, protfilter=2)

        # keep reading until we gathered all keys in this chunk,
        # or 300 ms of silence after the last VALGET
        deadline = time.time() + 3.0
        last_rx = None
        while time.time() < deadline:
            try:
                raw, parsed = rdr.read()
            except UBX_EXC:
                continue
            if parsed and getattr(parsed, "identity", "") == "CFG-VALGET":
                _merge_cfg_results(results, parsed)
                last_rx = time.time()
                if all(k in results for k in chunk):
                    break
                cfg = getattr(parsed, "cfgData", None) or getattr(parsed, "cfgdata", None)
                if isinstance(cfg, dict):
                    _merge_cfg_results(results, cfg)
                else:
                    # fallback: keyIDn / valn pairs
                    for attr, val in parsed.__dict__.items():
                        if attr.startswith("keyID"):
                            idx = attr[5:]
                            kval = val
                            v = parsed.__dict__.get("val" + idx) or parsed.__dict__.get("value" + idx)
                            if v is not None:
                                results[kval] = v
                last_rx = time.time()
                # stop early if we got all keys for this chunk
                if all(k in results for k in chunk):
                    break
            elif last_rx and (time.time() - last_rx) > 0.3:
                # quiet for 300 ms after last VALGET => assume done
                break
    return results


def load_regdesc_csv(path: str):
    """
    Load  CSV into two dicts:
      by_id[int_keyid]  -> {name, id, dtype, scale, unit, default, desc}
      by_name[str_name] -> same
    """
    by_id, by_name = {}, {}
    with open(path, newline="") as f:
        rdr = csv.reader(f)
        for row in rdr:
            if not row or row[0].strip().startswith("#"):
                continue
            # tolerate headers
            if row[0].upper().startswith("CFG") and len(row) >= 2:
                name = _norm_name(row[0])
                id_hex = row[1].strip()
            else:
                continue
            try:
                kid = int(id_hex, 16)
            except Exception:
                # if someone put "CFG_0x...." in col 0 instead
                m = _HEXKEY_RE.match(_norm_name(row[0]))
                kid = int(m.group(1), 16) if m else None
            if kid is None:
                # last resort: resolve name via pyubx2
                try:
                    kid, _ = cfgname2key(name)
                except Exception:
                    continue

            dtype   = row[2].strip() if len(row) > 2 else ""
            scale   = row[3].strip() if len(row) > 3 else ""
            unit    = row[4].strip() if len(row) > 4 else ""
            default = row[5].strip() if len(row) > 5 else ""
            desc    = row[6].strip() if len(row) > 6 else ""

            entry = {"name": name, "id": kid, "dtype": dtype,
                     "scale": scale, "unit": unit, "default": default, "desc": desc}
            by_id[kid] = entry
            by_name[name] = entry
    return by_id, by_name


def build_tmode_fixed_from_json(pos: dict, acc_m: float):
    """
    Add position coordinates to the list of registers to set. Needed for ZED-F9T
    where position is assumed fixed
    """
    fmt = pos.get("format","LLH").upper()
    items = []
    
    # MODE = FIXED, POS_TYPE = 0:ECEF, 1:LLH (per docs)
    items += [("CFG_TMODE_MODE", 2)]                      # 2 = FIXED
    items += [("CFG_TMODE_POS_TYPE", 1 if fmt=="LLH" else 0)]
    items += [("CFG_TMODE_FIXED_POS_ACC", int(round(acc_m * 10000)))]  # meters -> 0.1 mm?

    if fmt == "LLH":
        lat_i, lat_hp, lon_i, lon_hp, h_cm, h_hp = _split_scaled_llh(
            float(pos["lat_deg"]), float(pos["lon_deg"]), float(pos["height_m"])
        )
        items += [
          ("CFG_TMODE_LAT",       lat_i),
          ("CFG_TMODE_LON",       lon_i),
          ("CFG_TMODE_HEIGHT",    h_cm),
          ("CFG_TMODE_LAT_HP",    lat_hp),
          ("CFG_TMODE_LON_HP",    lon_hp),
          ("CFG_TMODE_HEIGHT_HP", h_hp),
        ]
    else:
        # ECEF (Earth-centered Earth-fixed path): Supply x/y/z (m), convert to cm + 0.1mm HP like above
        # LLH: (Latitude longitude height)
        raise NotImplementedError("ECEF path not shown—LLH is simplest")
    return items


def describe_plan(cfg_items, db_by_id, db_by_name):
    """
    Print a doc line for every key we're about to write, including
    engineering units using CSV scale+unit.
    """
    confg_gnss_logger.info("Descriptions for planned writes:")
    for it in cfg_items:
        name = _norm_name(it["name"])
        kid  = it["id"]
        val  = it["value"]
        dtype = it.get("dtype","")
        entry = db_by_id.get(kid) or db_by_name.get(name)
        if not entry:
            confg_gnss_logger.info(f"  {name:32s} id={hex(kid)} dtype={dtype:>2s}  value={val}  (no CSV description)")
            continue

        eng_val, eng_unit = _fmt_eng(val, dtype, entry.get("scale",""), entry.get("unit",""))
        # default, if present, also in eng units
        def_raw = entry.get("default","")
        def_txt = ""
        if def_raw not in ("", "-"):
            dv = _to_float(def_raw)
            ev, eu = _fmt_eng(dv if dv is not None else def_raw,
                              entry.get("dtype",""), entry.get("scale",""), entry.get("unit",""))
            def_txt = f" (default {ev}{eu})"

        desc = entry.get("desc","")
        confg_gnss_logger.info(f"  {name:32s} id={hex(kid)} dtype={entry.get('dtype',''):>2s} "
              f"raw={val}  eng={eng_val}{eng_unit} -> {desc}{def_txt}")


def initial_probe(ser, verbose=False):
    """
    Poll a couple of registers to check if things are alive
    """
    # MON-VER
    info = poll_mon_ver(ser)
    if verbose:
        def _clean(b): return b.decode(errors="ignore").strip("\x00")
        confg_gnss_logger.info(f"[MON-VER] model={_clean(info['extensions'][3])} prot={_clean(info['extensions'][2])} fw={_clean(info['extensions'][1])}")

    # Prove VALGET flow using wildcards and a single key
    uart_default = poll_one_by_id(ser, 0x4052FFFF, layer=7)  # UART1 group defaults
    uart_ram     = poll_one_by_id(ser, 0x4052FFFF, layer=0)  # UART1 group RAM
    confg_gnss_logger.debug(f"[VALGET] UART1 DEFAULT keys={len(uart_default)} RAM keys={len(uart_ram)}")

    # A couple of timepulse fields (if present)
    tp1 = poll_one_by_id(ser, 0x40050024, layer=0)  # CFG_TP_FREQ_TP1
    tp2 = poll_one_by_id(ser, 0x40050026, layer=0)  # CFG_TP_FREQ_TP2
    confg_gnss_logger.debug(f"[VALGET] TP1 freq: {tp1.get(0x40050024)}, TP2 freq: {tp2.get(0x40050026)}")

def poll_one_by_id(ser, keyid: int, layer: int = 0, pos: int = 0, timeout=3.0):
    """
    Poll a single register
    """
    # Correct way: build CFG-VALGET with payload using the helper
    msg = UBXMessage.config_poll(layer=layer, position=pos, keys=[keyid])
    confg_gnss_logger.info("TX: {msg} {msg.serialize().hex()}")
    ser.write(msg.serialize())

    rdr = UBXReader(ser, protfilter=2)
    t0 = time.time()
    out = {}
    while time.time() - t0 < timeout:
        try:
            raw, parsed = rdr.read()
        except UBX_EXC:
            continue
        if not parsed:
            continue
        confg_gnss_logger.info(f"RX: {parsed.identity} {getattr(parsed, 'length', None)}")
        if parsed.identity == "CFG-VALGET":
            out = {}
            _merge_cfg_results(out, parsed)
            valget_merged = {hex(k): v for k, v in out.items()}
            confg_gnss_logger.info(f"VALGET merged: {valget_merged}")
            return out
        elif parsed.identity == "ACK-NAK":
            raise RuntimeError("Device NAKed CFG-VALGET (bad payload or unsupported key).")
    return out

'''
def poll_cfg_layers(ser, key_ids):
    """
    Poll registers across a layer 
    """
    winners = {}           # keyID -> (value, layer_name)
    remaining = set(key_ids)
    for lname in LAYER_ORDER:
        if not remaining:
            break
        got = poll_cfg(ser, list(remaining), lname)
        for kid, val in got.items():
            winners[kid] = (val, lname)
        remaining -= set(got.keys())
    return winners
'''

def detect_model(ser) -> str:
    """
    Get the model of the device being used (way to distinguish between F9P and F9T)
    """
    info = poll_mon_ver(ser) 
    mods = [x for x in info["extensions"] if isinstance(x, (bytes, bytearray)) and x.startswith(b"MOD=")]
    mod = mods[0][4:].decode(errors="ignore").strip("\x00") if mods else ""
    return mod or "UNKNOWN"


def get_f9t_unique_id(ser: serial.Serial, timeout: float = 3.0) -> str:
    """
    Polls and returns the 5-byte ZED-F9T unique ID as a lowercase hex string.
    """
    # Build the poll request for UBX-SEC-UNIQID (Class 0x27, ID 0x03)
    SEC_CLASS = 0x27
    UNIQID_ID = 0x03
    msg = UBXMessage(SEC_CLASS, UNIQID_ID, POLL)

    try:
        ser.reset_input_buffer()
    except Exception:
        pass
    ser.write(msg.serialize())

    rdr = UBXReader(ser, protfilter=2)
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            (_raw, parsed) = rdr.read()
        except UBX_EXC:
            continue

        if not parsed:
            continue

        if getattr(parsed, "identity", "") == "SEC-UNIQID":
            uid = getattr(parsed, "uniqueId", None)

            # Handle case where pyubx2 parses the ID as a byte array
            if isinstance(uid, (bytes, bytearray)) and len(uid) == 5:
                return uid.hex().upper()
            # Handle case where pyubx2 parses the ID as an integer
            elif isinstance(uid, int):
                # Format as a 10-character hex string (5 bytes)
                return f'{uid:010x}'.upper()
            else:
                # This case handles unexpected payload structures
                raise RuntimeError(
                    "SEC-UNIQID received, but 'uniqueId' attribute was missing, "
                    f"of wrong type, or wrong length. Got: {uid!r}"
                )
    # If the loop finishes without returning, we timed out
    raise RuntimeError("Timeout waiting for SEC-UNIQID response from device.")


# add helper
def lock_config(ser, layers: list[str]) -> list[bool]:
    """
    Set CFG_SEC_CFG_LOCK=1 on each requested layer in order.
    Returns a list of ACK booleans, one per layer lock attempt.
    """
    from pyubx2.ubxhelpers import cfgname2key
    kid, _ = cfgname2key("CFG_SEC_CFG_LOCK")
    acks = []
    for lname in layers:
        mask = SET_LAYER[lname.upper()]  # your existing dict: {"RAM":1,"BBR":2,"FLASH":4}
        msg = UBXMessage.config_set(layers=mask, transaction=0, cfgData=[(kid, 1)])
        ser.write(msg.serialize())
        acks.append(_await_ack(ser, timeout=2.5))
    return acks

class _HelpFormatter(argparse.ArgumentDefaultsHelpFormatter,
                     argparse.RawDescriptionHelpFormatter):
    pass

def parse_args():
    ap = argparse.ArgumentParser(
        prog="conf_gnss",
        description="Configure u-blox GNSS receivers (F9T/F9P) from a JSON plan and verify\n Currently only verifies a single layer",
        epilog=(
            "Examples:\n"
            "  python config_gnss.py conf_gnss.json5\n"
            "  python config_gnss.py -v --verify-layer RAM config.json\n"
            "  python config_gnss.py -vv --desc-csv regs.csv config.json\n"
            "  python config_gnss.py --probe-only\n"
        ),
        formatter_class=_HelpFormatter,
    )

    # Positional
    ap.add_argument("json", nargs="?", help="Path to configuration JSON")

    # Common flags
    ap.add_argument("-v", "--verbose", action="count", default=0,
                    help="Increase verbosity (-v, -vv)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse & resolve, print plan, do not write")
    ap.add_argument("--probe-only", action="store_true",
                    help="Probe device (MON-VER, sample VALGET) and exit")
    ap.add_argument("--verify-layer", choices=["RAM","BBR","FLASH","DEFAULT"],
                    help="Layer to poll for verification (overrides JSON)")
    ap.add_argument("--desc-csv", help="CSV with register descriptions for -vv")

    # If you want “show help when no args” behavior:
    if len(sys.argv) == 1:
        ap.print_help()
        sys.exit(0)

    return ap.parse_args()

def main():
    """
    Set options, load JSON configuration file, write/check registers
    """
   
    args = parse_args()

    with open(args.json, "r") as f:
        doc = json.load(f)

    port = doc.get("port", "/dev/ttyACM0")
    baud = int(doc.get("baud", 115200))
    apply_layers = doc.get("apply_to_layers", ["RAM"])
    verify_layer = doc.get("verify_layer", "RAM")
    cfg_entries = doc["config"]
    register_csv = doc["register_csv"] if not(args.desc_csv) else args.desc_csv
    pos = doc.get("position")
    verify_layer = (args.verify_layer or doc.get("verify_layer","RAM")).upper() #Note you may get NAK VALGET if reading from flash
    
    cfg_items = _to_cfg_items(cfg_entries)   # [{'name','id','value'}, ...]
    set_layers_mask = _layers_mask(apply_layers)

    # Open serial
    with serial.Serial(port, baudrate=baud, timeout=0.5) as ser:
        ser.reset_input_buffer()
        ser.reset_output_buffer()

        model = detect_model(ser)
        confg_gnss_logger.info(f"Detected: {model}")

        if model.startswith("ZED-F9T") and pos:
            if args.verbose:
                confg_gnss_logger.info("Setting coordinate positions from JSON")
            tmode_pairs = build_tmode_fixed_from_json(pos, pos.get("acc_m", 0.05))
            tmode_items = _to_cfg_items([{"key": k, "value": v} for k, v in tmode_pairs])
            cfg_items.extend(tmode_items)

        # BEFORE values (for diff) — take this after cfg_items is finalized, before any writes
        before_map = poll_cfg(ser, [it["id"] for it in cfg_items], verify_layer) if args.verbose else {}

        if args.verbose >= 2:
            try:
                db_by_id, db_by_name = load_regdesc_csv(register_csv)
                describe_plan(cfg_items, db_by_id, db_by_name)
            except Exception as e:
                confg_gnss_logger.warning(f"[WARN] Could not load descriptions from {register_csv}: {e}")

        if args.verbose or args.probe_only:
            initial_probe(ser, verbose=True)
            if args.probe_only:
                return

        if args.verbose:
            _log_plan(cfg_items)
        if args.dry_run:
            return

        # Send (grouped) and verify
        acks = send_cfg_valset_grouped(ser, cfg_items, set_layers_mask, verbose=args.verbose)
        confg_gnss_logger.info(f"ACKs per chunk: {acks}")
        if not all(acks):
            confg_gnss_logger.warning("[WARN] One or more CFG-VALSET batches were NAKed.")

        # --- VERIFY (poll exactly the requested layer) ---
        key_ids = [it["id"] for it in cfg_items]
        reported = poll_cfg(ser, key_ids, verify_layer)

    # --- Compare (by ID) ---
    failures = []
    for it in cfg_items:
        kid   = it["id"]
        want  = it["value"]
        got   = reported.get(kid)
        dtype = DTYPE_BY_ID.get(kid)
        ok    = (got == want)

        if not args.verbose:
            status = "OK" if ok else f"MISMATCH (wanted {want}, got {got})"
            confg_gnss_logger.debug(f"{it['name']:36s} : {status} [{verify_layer}]")
        else:
            before_val = before_map.get(kid)
            line  = f"{it['name']:36s} : {'OK' if ok else 'MISMATCH'} [{verify_layer}]"
            line += f"  want={_fmt_val(want, dtype)}"
            line += f"  got={_fmt_val(got, dtype)}"
            line += f"  before={_fmt_val(before_val, dtype)}"
            confg_gnss_logger.info(line)

        if not ok:
            failures.append((it["name"], want, got))

    if failures:
        sys.exit(2)
    else:
        confg_gnss_logger.info("All settings applied and verified.")
    # Running this will lock configuration until a power cycle
    '''
    layers_to_lock = ["BBR"]  # add "RAM" to freeze this session too; avoid "FLASH" until final build
    acks = lock_config(ser, layers_to_lock)
    print("Config lock ACKs:", acks)
    '''

if __name__ == "__main__":
    main()