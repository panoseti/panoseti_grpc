#! /usr/bin/env python3

# functions to read and parse config files

import json
import os
import sys
from typing import Any

obs_config_filename = "obs_config.json"
daq_config_filename = "daq_config.json"
data_config_filename = "data_config.json"
quabo_uids_filename = "quabo_uids.json"
quabo_info_filename = "../quabos/quabo_info.json"
detector_info_filename = "../quabos/detector_info.json"
quabo_calib_filename = "../quabos/detovervol_%dv/%s/quabo_calib_%s.json"
pointing_filename = "pointing.json"
quabo_ph_baseline_filename = "quabo_ph_baseline.json"
sw_info_filename = "sw_info.json"
quabo_config_filename = "quabo_config_*.json"
network_config_filename = "network_config.json"
# list of config files copied to data dir
config_file_names = [
    obs_config_filename,
    daq_config_filename,
    data_config_filename,
    quabo_uids_filename,
    quabo_ph_baseline_filename,
    sw_info_filename,
    quabo_config_filename,
    network_config_filename,
]


# compute a 'module ID', given its base quabo IP addr: bits 2..9 of IP addr
#
def ip_addr_to_module_id(ip_addr_str: str) -> int:
    pieces = ip_addr_str.split(".")
    n = int(pieces[3]) + 256 * int(pieces[2])
    return (n >> 2) & 255


# given module base IP address, return IP addr of quabo i
#
def quabo_ip_addr(base: str, i: int) -> str:
    x = base.split(".")
    x[3] = str(int(x[3]) + i)
    return ".".join(x)


def get_boardloc(module_ip_addr: str, quabo_index: int) -> int:
    """Given a module ip address and a quabo index, returns the BOARDLOC of
    the corresponding quabo."""
    pieces = module_ip_addr.split(".")
    boardloc = int(pieces[2]) * 256 + int(pieces[3]) + quabo_index
    return boardloc


# assign sequential numbers to domes,
# and IDs to modules
#
def assign_numbers(c: Any) -> None:
    for ndome, dome in enumerate(c["domes"]):
        dome["num"] = ndome
        for module in dome["modules"]:
            module["id"] = ip_addr_to_module_id(module["ip_addr"])


# input: a string of the form "0-2, 5-6"
# output: a list of integers, e.g. 0,1,2,5,6
#
def string_to_list(s: str) -> list[int]:
    out = []
    parts = s.split(",")
    for part in parts:
        nums = part.split("-")
        if len(nums) > 1:
            a = int(nums[0])
            b = int(nums[1])
            for i in range(a, b + 1):
                out.append(i)
        else:
            out.append(int(nums[0]))
    return out


# in DAQ node objects, expand module range strings
# to list of module numbers
#
def expand_ranges(daq_config: Any) -> None:
    for node in daq_config["daq_nodes"]:
        node["module_ids"] = string_to_list(node["module_ids"])


# given a module ID, find the DAQ node that's handling it
#
def module_id_to_daq_node(daq_config: Any, module_id: int) -> Any:
    for node in daq_config["daq_nodes"]:
        if module_id in node["module_ids"]:
            return node
    raise Exception(f"no DAQ node is handling module {module_id}")


def check_config_file(name: str, dir: str = ".") -> None:
    if not os.path.exists(f"{dir}/{name}"):
        print(f"The config file '{name}' doesn't exist.")
        print(f"Create a symbolic link from {name} to a specific config file, e.g.:")
        print("   ln -s {}_lick.json {}".format(name.split(".")[0], name))

        sys.exit()


def get_obs_config(dir: str = ".") -> Any:
    check_config_file(obs_config_filename, dir)
    with open(f"{dir}/{obs_config_filename}") as f:
        s = f.read()
    c = json.loads(s)
    assign_numbers(c)
    return c


def get_daq_config() -> Any:
    check_config_file(daq_config_filename)
    with open(daq_config_filename) as f:
        s = f.read()
    c = json.loads(s)
    expand_ranges(c)
    return c


def get_data_config(dir: str = ".") -> Any:
    path = f"{dir}/{data_config_filename}"
    check_config_file(data_config_filename, dir)
    with open(path) as f:
        c = f.read()
    conf = json.loads(c)
    if "flash_params" in conf:
        fp = conf["flash_params"]
        if fp["rate"] > 7:
            raise Exception(f"flash rate > 7 in {data_config_filename}")
        if fp["level"] > 31:
            raise Exception(f"flash level > 31 in {data_config_filename}")
        if fp["width"] > 15:
            raise Exception(f"flash width > 15 in {data_config_filename}")
    return conf


def get_network_config(dir: str = ".") -> Any:
    path = f"{dir}/{network_config_filename}"
    # as the network config file is not designed to the users,
    # we check it manually, instead of using check_config_file.
    try:
        with open(path) as f:
            c = f.read()
        conf = json.loads(c)
    except Exception:
        print("***********Warning: No network config file! **************")
        print("******All the devices should be in the same subnet *******")
        conf = {}
    return conf


def get_quabo_uids() -> Any:
    if not os.path.exists(quabo_uids_filename):
        print(f"{quabo_uids_filename} is missing.  Run get_uids.py")
        sys.exit()
    with open(quabo_uids_filename) as f:
        s = f.read()
    c = json.loads(s)
    assign_numbers(c)
    return c


# get detector info as an array indexed by serialno
#
def get_detector_info() -> dict[str, float]:
    check_config_file(detector_info_filename)
    with open(detector_info_filename) as f:
        s = f.read()
    c = json.loads(s)
    d = {}
    with open(obs_config_filename) as f:
        s = f.read()
    obs_config = json.loads(s)
    for det in c:
        try:
            d[str(det["serialno"])] = float(det["operating_voltage"])
        except Exception:
            d[str(det["serialno"])] = float(det["breakdown_voltage"]) + obs_config["detector_overvoltage"]
    return d


# get quabo info as an array indexed by uid
#
def get_quabo_info() -> dict[str, Any]:
    check_config_file(quabo_info_filename)
    with open(quabo_info_filename) as f:
        s = f.read()
    c = json.loads(s)
    d = {}
    for q in c:
        d[q["uid"]] = q
    return d


def get_quabo_ph_baselines() -> Any:
    check_config_file(quabo_ph_baseline_filename)
    with open(quabo_ph_baseline_filename) as f:
        s = f.read()
    c = json.loads(s)
    return c


# get quabo calibration info
#
def get_quabo_calib(serialno: Any, detovervol: Any, mode: Any) -> Any:
    # print('reading calib file %s'%serialno)
    path = quabo_calib_filename % (detovervol, mode, serialno)
    with open(path) as f:
        s = f.read()
    return json.loads(s)


# return list of modules from obs_config
#
def get_modules(c: Any) -> list[Any]:
    modules = []
    for dome in c["domes"]:
        for module in dome["modules"]:
            modules.append(module)
    return modules


# link modules to DAQ nodes:
# - in the daq_config data structure, add a list "modules"
#   to each daq node object, of the module objects
#   in the quabo_uids data structure;
# - in the quabo_uids data structure, in each module object,
#   add a link "daq_node" to the DAQ node that's handling it.
#
def associate(daq_config: Any, quabo_uids: Any) -> None:
    for node in daq_config["daq_nodes"]:
        node["modules"] = []
    for dome in quabo_uids["domes"]:
        for module in dome["modules"]:
            daq_node = module_id_to_daq_node(daq_config, module["id"])
            daq_node["modules"].append(module)
            module["daq_node"] = daq_node


# show which module is going to which data recorder
#
def show_daq_assignments(quabo_uids: Any) -> None:
    for dome in quabo_uids["domes"]:
        for module in dome["modules"]:
            ip_addr = module["ip_addr"]
            daq_node = module["daq_node"]
            for i in range(4):
                q = module["quabos"][i]
                print(
                    "data from quabo {} ({}) -> DAQ node {}".format(
                        q["uid"], quabo_ip_addr(ip_addr, i), daq_node["ip_addr"]
                    )
                )


if __name__ == "__main__":
    c = get_detector_info()
    print(c)
