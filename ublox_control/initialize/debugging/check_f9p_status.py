"""
ubxpoller.py

This example illustrates how to read, write and display UBX messages
"concurrently" using threads and queues. This represents a useful
generic pattern for many end user applications.

Usage:

python3 ubxpoller.py port="/dev/ttyACM0" baudrate=38400 timeout=0.1

It implements two threads which run concurrently:
1) an I/O thread which continuously reads UBX data from the
receiver and sends any queued outbound command or poll messages.
2) a process thread which processes parsed UBX data - in this example
it simply prints the parsed data to the terminal.
UBX data is passed between threads using queues.

Press CTRL-C to terminate.

FYI: Since Python implements a Global Interpreter Lock (GIL),
threads are not strictly concurrent, though this is of minor
practical consequence here.

Created on 07 Aug 2021

:author: semuadmin
:copyright: SEMU Consulting © 2021
:license: BSD 3-Clause
"""

from queue import Queue
from sys import argv
from threading import Event, Thread
from time import sleep

from serial import Serial

from pyubx2 import POLL, UBX_PAYLOADS_POLL, UBX_PROTOCOL, UBXMessage, UBXReader
from pygnssutils.gnssntripclient import GNSSNTRIPClient, NTRIP2, RTCM

def get_vacc_mm_from_navpvt(pkt: bytes) -> int:
    # UBX header check
    if pkt[:2] != b'\xb5\x62' or pkt[2:4] != b'\x01\x07':
        raise ValueError("Not UBX-NAV-PVT")
    length = int.from_bytes(pkt[4:6], 'little')
    if length != 92:
        raise ValueError("Unexpected NAV-PVT length")
    payload = pkt[6:6+length]
    # vAcc is U4 (mm) at payload[44:48]
    return int.from_bytes(payload[44:48], 'little', signed=False)


def io_data(
    ubr: UBXReader,
    readqueue: Queue,
    sendqueue: Queue,
    stop: Event,
    ser
):
    """
    THREADED
    Read and parse inbound UBX data and place
    raw and parsed data on queue.

    Send any queued outbound messages to receiver.
    """
    # pylint: disable=broad-exception-caught
    state = {}
    
    """
    Start an NTRIP client and stream RTCM bytes to 'port' at 'baud'.
    Blocks until interrupted (Ctrl-C) or client stops.
    Required settings keys: server, port, mountpoint, ntripuser, ntrippassword.
    """
    # Make sure pygnssutils is available
    baudrate = 115200
    settings = {
        "server": "132.239.152.4",
        "port": 2105,
        "mountpoint": "P479_RTCM3",
        "https": 0,               
        "ntripversion": "2.0",
        "datatype": "RTCM",        
        "ntripuser": "",
        "ntrippassword": "",
        "ggainterval": 15,        
        "ntripggamode":1,             
        "reflat": 37.8731,    
        "reflon": -122.2571}
    # --- extract settings ---
    server      = settings["server"]
    cport       = int(settings["port"])
    mountpoint  = settings["mountpoint"]
    https       = int(settings.get("https", 0))
    ntripver_in = str(settings.get("ntripversion", "2.0")).strip()
    ntripver    = NTRIP2 if ntripver_in in ("2.0", "2", "NTRIP2") else ntripver_in
    datatype_in = str(settings.get("datatype", "RTCM")).upper()
    datatype    = RTCM if datatype_in == "RTCM" else datatype_in
    user        = settings["ntripuser"]
    password    = settings["ntrippassword"]
    ggainterval = int(settings.get("ggainterval", 15))
    reflat      = settings.get("reflat", None)
    reflon      = settings.get("reflon", None)
    refalt      = settings.get("refalt", None)
    ggamode     = int(settings.get("ggamode", 1 if (reflat is not None and reflon is not None) else 0))
    verbosity   = int(settings.get("verbosity", 0))

    kw = dict(
        server=server, port=cport, mountpoint=mountpoint,
        https=https, version=ntripver, datatype=datatype,
        ntripuser=user, ntrippassword=password,
        ggainterval=ggainterval, ggamode=ggamode,
        verbosity=verbosity,
    )
    if reflat is not None: kw["reflat"] = float(reflat)
    if reflon is not None: kw["reflon"] = float(reflon)
    if refalt is not None: kw["refalt"] = float(refalt)
    ser.reset_input_buffer(); ser.reset_output_buffer()
    msg = UBXMessage(0x06, 0x71, msgmode=1, mode=0, ecefXOrLat=0, ecefYOrLon=0, ecefZOrAlt=0,
                    ecefXOrLatHP=0, ecefYOrLonHP=0, ecefZOrAltHP=0, fixedPosAcc=0, svinMinDur=0, svinAccLimit=0)
    ser.write(msg.serialize())
    # --- start NAV-PVT watcher (for RTK stats) ---
    fix  = {}
    t_watch = None

    # --- monkey-patch write() to count bytes (no wrapper needed) ---
    '''
    bytes_written = {"n": 0}
    orig_write = ser.write
    def counting_write(b):
        n = orig_write(b)
        bytes_written["n"] += n
        return n
    ser.write = counting_write  # <-- patch
    '''
    # --- run NTRIP client ---
    with GNSSNTRIPClient() as gnc:
        print(f"[NTRIP] ggamode={ggamode} ggainterval={ggainterval} reflat={reflat} reflon={reflon}")
        print(f"[NTRIP] Connecting to {server}:{cport} mp={mountpoint} tls={https} -> {cport}@{baudrate}")
        ok = gnc.run(output=ser, **kw)
        if not ok:
            print("[NTRIP] Client failed to start (check creds/mountpoint).", file=sys.stderr)
            return
        try:
            # last_n = 0
            # last_t = time.time()
            while not getattr(gnc, "stopevent", None).is_set():
                (raw_data, parsed_data) = ubr.read()
                p = parsed_data
                if parsed_data:
                    if getattr(p, "identity", "") == "NAV-PVT":     
                        state["carrSoln"] = getattr(p, "carrSoln", None)   # 0/1/2
                        state["hAcc"]     = getattr(p, "hAcc", None)       # mm
                        state["vAcc"]     = get_vacc_mm_from_navpvt(raw_data)
                        state["fixType"] = getattr(p, "fixType", None)
                        state["numSV"]   = getattr(p, "numSV", None)
                        state["pDOP"]    = getattr(p, "pDOP", None)   # 0.01 units (e.g. 123 => 1.23)
                        state["locked"] = getattr(p, "gnssFixOk", None)
                        state['lat'] = getattr(p, 'lat', None)
                        state['lon'] = getattr(p, 'lon', None)
                        state['height'] = getattr(p, 'height', None)
                        print(f"numSV: {state['numSV']}, hAcc (mm): {state['hAcc']}, vAcc (mm): {state['vAcc']}, fix type:  {state['fixType']}, locked: {state['locked']}")
                    
                        print(f"Latitude: {state['lat']}, Longitude: {state['lon']}, Altitude: {state['height']}")
                    #readqueue.put((raw_data, parsed_data))

                # refine this if outbound message rates exceed inbound
                # now = time.time()
                # n = bytes_written["n"]
                # bps = (n - last_n) / max(0.001, now - last_t)
                # last_n, last_t = n, now

                # cs  = fix.get("carrSoln")
                # ha  = fix.get("hAcc")
                # va = fix.get("vAcc")
                # cs_s = {0: "none", 1: "float", 2: "fixed"}.get(cs, "n/a")
                # ha_s = f"{ha/1000:.2f} m" if isinstance(ha, (int, float)) else "n/a"
                # va_s = f"{va/1000:.2f} m" if isinstance(va, (int, float)) else "n/a"
                # print(f"[NTRIP] {bps:.0f} B/s -> {port} | RTK={cs_s} hAcc={ha_s}, vAcc={va_s}")
                # ft = fix.get("fixType"); sv = fix.get("numSV"); pd = fix.get("pDOP")
                # pd_s = f"{pd:.2f}" if isinstance(pd,(int,float)) else "n/a"
                # print(f"... | fixType={ft} numSV={sv} pDOP={pd_s}")
            #except KeyboardInterrupt:
            #    pass
        except Exception as err:
            print(f"\n\nSomething went wrong - {err}\n\n")
            #continue


def process_data(queue: Queue, stop: Event):
    """
    THREADED
    Get UBX data from queue and display.
    """

    while not stop.is_set():
        if queue.empty() is False:
            (_, parsed) = queue.get()
            print(parsed)
            queue.task_done()


def main(**kwargs):
    """
    Main routine.
    """

    port = kwargs.get("port", "/dev/ttyACM0")
    baudrate = int(kwargs.get("baudrate", 115200))
    timeout = float(kwargs.get("timeout", 0.1))
    read_queue = Queue()
    send_queue = Queue()
    stop_event = Event()

    with Serial(port, baudrate, timeout=timeout) as stream:
        ubxreader = UBXReader(stream, protfilter=UBX_PROTOCOL)
        stop_event.clear()
        io_thread = Thread(
            target=io_data,
            args=(
                ubxreader,
                read_queue,
                send_queue,
                stop_event,
                stream
            ),
            daemon=True,
        )
        process_thread = Thread(
            target=process_data,
            args=(
                read_queue,
                stop_event,
            ),
            daemon=True,
        )

        print("\nStarting handler threads. Press Ctrl-C to terminate...")
        io_thread.start()
        process_thread.start()

        # loop until user presses Ctrl-C
        while not stop_event.is_set():
            try:
                # DO STUFF IN THE BACKGROUND...
                # poll all available NAV messages (receiver will only respond
                # to those NAV message types it supports; responses won't
                # necessarily arrive in sequence)
                count = 0
                for nam in UBX_PAYLOADS_POLL:
                    if nam[0:4] == "NAV-":
                        print(f"Polling {nam} message type...")
                        msg = UBXMessage("NAV", nam, POLL)
                        send_queue.put(msg)
                        count += 1
                        sleep(1)
                stop_event.set()
                print(f"{count} NAV message types polled.")

            except KeyboardInterrupt:  # capture Ctrl-C
                print("\n\nTerminated by user.")
                stop_event.set()

        print("\nStop signal set. Waiting for threads to complete...")
        io_thread.join()
        process_thread.join()
        print("\nProcessing complete")


if __name__ == "__main__":

    main(**dict(arg.split("=") for arg in argv[1:]))
