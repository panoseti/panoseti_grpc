# UbloxControl Service

The `UbloxControl` service provides a high-performance gRPC interface for configuring and streaming data from u-blox ZED-F9T timing modules. It is designed to give remote clients a simple control over the hardware.

The typical workflow involves two main steps:

1. A client sends an `InitF9t` request containing a detailed configuration to set up the ZED-F9T module. The server uses this to connect to the correct serial device, apply settings for GNSS constellations and timing signals, and verify the chip's identity.
2. Once initialized, the client calls the `CaptureUblox` RPC to subscribe to a real-time stream of UBX protocol messages, such as timing and navigation data.

## Core Remote Procedure Calls

The service exposes two primary RPCs for interacting with the ZED-F9T chip.

### `InitF9t`

This RPC is the entry point for configuring the hardware. It is a unary call where the client sends a single request and receives a single response.

`rpc InitF9t(InitF9tRequest) returns (InitF9tResponse)`

**Functionality:**

* **Connects and Verifies**: The server connects to the ZED-F9T using the serial device path specified in the configuration (e.g., `/dev/ttyACM1`). It then polls the chip to detect its model and unique hardware ID, ensuring it matches the client's expected configuration.
* **Applies Configuration**: It applies a list of configuration key-value pairs to the device registers. This includes setting up GNSS signal processing (e.g., enabling GPS L1/L2), configuring the timepulse outputs (TP1/TP2), and enabling specific UBX message types like `TIM-TP` and `NAV-TIMEUTC`.
* **Verifies Settings**: After applying the settings, the server polls the device again to verify that all configuration values were written correctly to the specified memory layer (e.g., RAM).
* **Starts I/O**: On success, the server starts a persistent background task that continuously reads data from the serial port and caches the latest UBX messages.

The request contains the F9T configuration and a `force_init` flag, which, if true, will terminate any existing client streams before re-initializing the chip.

### `CaptureUblox`

This RPC establishes a persistent, server-side stream for receiving real-time data from an already-initialized F9T chip.

`rpc CaptureUblox(CaptureUbloxRequest) returns (stream CaptureUbloxResponse)`

**Functionality:**

* **Client Subscription**: A client calls this RPC to subscribe to data. The request can include an array of regular expression `patterns` to filter for specific message types (e.g., `[".*"]` to receive all messages, or `["TIM-TP", "NAV-TIMEUTC"]` for specific ones).
* **Initial Cache Broadcast**: Upon connection, the server immediately sends the client all currently cached UBX messages that match the requested patterns. This ensures the client has the most recent state without having to wait for new messages.
* **Real-time Streaming**: After the initial broadcast, the server streams new UBX messages to the client in real-time as they are read from the hardware.

Each `CaptureUbloxResponse` message contains the packet's identity (`name`), its raw byte `payload`, and a `parsed_data` structure with the decoded fields.

## The `f9t_config.json5` File
```json5
{
  // General config settings for all F9T chips
  "baud": 115200,
  "apply_to_layers": ["RAM", "BBR"], // Options: RAM, BBR (battery backup RAM), flash, default?
  "verify_layer": "RAM",
  "register_csv": "../initialize/ZED-F9T_Registers.csv",


  "cfg_key_settings": [
    // --- Constellations & signals ---
    { "key": "CFG_SIGNAL_GPS_ENA", "value": 1 },

    // UBX-TIM-TP (qErr) at 1 Hz on USB and/or UART1
    { "key": "CFG_MSGOUT_UBX_TIM_TP_USB", "value": 1 },
    { "key": "CFG_MSGOUT_UBX_TIM_TP_UART1", "value": 1 },
    { "key": "CFG_MSGOUT_UBX_NAV_TIMEUTC_USB",   "value": 1 },
    { "key": "CFG_MSGOUT_UBX_NAV_TIMEUTC_UART1", "value": 1 },
    
    // More keys here...
  ],

  // Chip-specific configuration information
  "f9t_chips": [
    {
      "f9t_uid": "DF03A241BC",
      "host": "localhost",// TODO: get this from daq_config.json + network_config.json
      "port": 50051,    // TODO: get this from daq_config.json + network_config.json
      "device": "/dev/ttyACM1",                         // TODO: replace with real device file
      "position": {
        "format": "LLH",              // "LLH" or "ECEF"
        "lat_deg": 37.4219999,        // degrees
        "lon_deg": -122.0840575,      // degrees
        "height_m": 12.345,           // meters (ellipsoidal)
        "acc_m": 0.02                 // 3D accuracy estimate (meters) for fixed mode
      }
    },
    // More chip configurations here...
  ]
}

```

The behavior of the `InitF9t` RPC is primarily defined by the `f9t_config.json5` file. This file uses the **JSON5 format**, which supports features like comments and trailing commas to improve readability.

The configuration is structured as follows:

* **Global Settings**: Top-level keys that apply to all chips, such as `baud` rate, `apply_to_layers` (which memory to write to, e.g., `["RAM", "BBR"]`), and `verify_layer` (which memory to read from for verification).
* **`cfg_key_settings`**: A list of objects, where each object defines a specific u-blox register to configure. The `"key"` is the register name (e.g., `CFG_SIGNAL_GPS_ENA`), and the `"value"` is the desired setting. These are used to control everything from which GNSS signals are enabled to the frequency and duty cycle of the timepulse outputs.
* **`f9t_chips`**: An array of objects, each defining a specific ZED-F9T device. This allows a single configuration file to manage multiple hardware units. Each object contains:
    * `f9t_uid`: The 10-digit unique ID of the chip, used for verification.
    * `device`: The filesystem path to the serial port (e.g., `/dev/ttyACM1`).
    * `position`: A critical object for timing applications. It specifies the antenna's fixed position in latitude, longitude, and height (`lat_deg`, `lon_deg`, `height_m`) and its accuracy (`acc_m`). This information is required for the F9T's time-only mode to function correctly.


## Basic Usage Example

The following Python example demonstrates how to use an asynchronous client to connect to the `UbloxControl` service, initialize the F9T, and stream data. This pattern is implemented in `simple_client.py`.

```python
import asyncio
import grpc
import copy
from panoseti_grpc.generated import ublox_control_pb2, ublox_control_pb2_grpc
from panoseti_grpc.ublox_control.resources import default_f9t_cfg  # Assumes a loaded config
from google.protobuf.json_format import ParseDict, MessageToDict
from google.protobuf.struct_pb2 import Struct

async def main():
    """
    Client workflow for connecting to UbloxControl, initializing the F9T,
    and capturing UBX data streams.
    """
    # Use a 'with' statement to ensure the gRPC channel is properly closed.
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        # 1. Prepare the InitF9t request from the configuration file.
        # This example uses the first chip defined in the f9t_chips list.
        chip_config = default_f9t_cfg['f9t_chips']
        
        # Create a complete config for this specific chip
        f9t_config_for_rpc = copy.deepcopy(default_f9t_cfg)
        del f9t_config_for_rpc['f9t_chips']
        f9t_config_for_rpc.update(chip_config)

        init_request = ublox_control_pb2.InitF9tRequest(
            f9t_config=ParseDict(f9t_config_for_rpc, Struct()),
            force_init=True  # Force re-initialization
        )

        # 2. Call the InitF9t RPC to configure the chip.
        try:
            init_response = await stub.InitF9t(init_request)
            print(f"InitF9t successful: {init_response.message}")
        except grpc.aio.AioRpcError as e:
            print(f"Error initializing F9T: {e.details()}")
            return

        # 3. Create a request to capture all UBX data.
        capture_request = ublox_control_pb2.CaptureUbloxRequest(
            patterns=[".*"]  # Use regex to match all message types
        )

        # 4. Listen to the data stream from the CaptureUblox RPC.
        print("Starting UBX data stream...")
        try:
            # The async for loop will process messages as they arrive.
            async for response in stub.CaptureUblox(capture_request):
                # The response object contains the raw payload and parsed data.
                parsed_dict = MessageToDict(response.parsed_data)
                print(f"Received message: {response.name}, "
                      f"Timestamp: {response.pkt_unix_timestamp.ToJsonString()}")
                # print(f"Parsed data: {parsed_dict}")

        except grpc.aio.AioRpcError as e:
            print(f"Data stream failed: {e.details()}")
        except KeyboardInterrupt:
            print("Stream stopped by user.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Client shut down.")
```
