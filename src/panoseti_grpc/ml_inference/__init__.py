"""
panoseti_grpc.ml_inference — Real-time ML prediction gRPC service.

This is a *thin gRPC adapter*.  All ML logic lives in ``panoseti_analysis``.
The servicer holds a set of active subscriber queues and fans each
``EmitPrediction`` call out to all open ``StreamPredictions`` and
``SubscribeAlerts`` streams.
"""
