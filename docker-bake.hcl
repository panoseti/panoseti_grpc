group "default" {
  targets = [
    "qa-linter",
    "daq-data-test",
    "daq-data-hashpipe-test",
    "daq-control-test",
    "telemetry-runner",
    "telemetry-client",
    "unified-server-test"
  ]
}

target "base-target" {
  context = "."
  dockerfile = "Dockerfile.ci"
}

target "qa-linter" {
  inherits = ["base-target"]
  target = "qa-linter"
}

target "daq-data-test" {
  inherits = ["base-target"]
  target = "daq-data-test"
}

target "daq-data-hashpipe-test" {
  inherits = ["base-target"]
  target = "daq-data-hashpipe-test"
}

target "daq-control-test" {
  inherits = ["base-target"]
  target = "daq-control-test"
}

target "telemetry-runner" {
  inherits = ["base-target"]
  target = "telemetry-runner"
}

target "telemetry-client" {
  inherits = ["base-target"]
  target = "telemetry-client"
}

target "unified-server-test" {
  inherits = ["base-target"]
  target = "unified-server-test"
}
