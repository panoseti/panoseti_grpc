
from panoseti_grpc.generated import daq_control_pb2
print(dir(daq_control_pb2))
import inspect
for name, obj in inspect.getmembers(daq_control_pb2):
    if inspect.isclass(obj):
        print(f"Class: {name}")
