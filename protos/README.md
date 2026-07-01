# Protos

## Contents

- [daq_control.proto](./daq_control.proto)
  - RPCs for controlling the Hashpipe lifecycle on DAQ nodes (start/stop/status/cleanup).
- [daq_data.proto](./daq_data.proto)
  - RPCs for streaming science data (images, pulse heights) from the DAQ node to the headnode.
- [ml_inference.proto](./ml_inference.proto)
  - RPCs for real-time ML cloud-detection predictions and pub-sub alerting.
- [telemetry.proto](./telemetry.proto)
  - RPCs for centralized logging and telemetry metrics aggregation.
- [ublox_control.proto](./ublox_control.proto)
  - RPCs for configuration of, and data streaming from, a ZED-F9T chip. 
