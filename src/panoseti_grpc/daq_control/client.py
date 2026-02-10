import grpc
from panoseti_grpc.generated import daq_control_pb2, daq_control_pb2_grpc

class DaqControlClient:
    """
    Client for the PANOSETI Daq Control Service.
    Supports both Strict (Production) and Flexible (Experimental) logging.
    """
    def __init__(self, host="localhost", port=50051):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = daq_control_pb2_grpc.DaqControlStub(self.channel)

    def StartDaq(self, parameters):
        """
        Docstring for StartDaq
        
        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * daq_ip_addr(str) - ip address
                    * bindhost(str) - ethernet port for receiving packets
                    * max_file_size_mb(uint32) - max file size in MB
                    * group_ph_frames(bool) - set if group ph frames from 4 Qubaos
                    * run_dir(str) - the new directory for this run, 
                                    which should be under data_dir
                    * obs(str) - obs name
                    * module_id (list) - modules for this daq node
        """
        # TODO: check if all of the parameters are reasonable
        #       We could use Pydantic for this.
        request = daq_control_pb2.StartDaqRequest()
        request.data_dir = parameters['data_dir']
        request.daq_ip_addr = parameters['daq_ip_addr']
        request.bindhost = parameters['bindhost']
        request.max_file_size_mb = parameters['max_file_size_mb']
        request.group_ph_frames = parameters['group_ph_frames']
        request.run_dir = parameters['run_dir']
        request.obs = parameters['obs']
        request.module_id.extend(parameters['module_id'])
        try:
            resp = self.stub.StartDaq(request)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
            # this return may not be necessary
            return resp.success
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}")
    
    def StopDaq(self, parameters):
        """
        Docstring for StopDaq
        
        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * run_dir(str) - the new directory for this run, 
                                    which should be under data_dir
        """
        # TODO: check if all of the parameters are reasonable
        #       We could use Pydantic for this.
        request = daq_control_pb2.StopDaqRequest()
        request.data_dir = parameters['data_dir']
        request.run_dir = parameters['run_dir']
        try:
            resp = self.stub.StopDaq(request)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
            # this return may not be necessary
            return resp.success
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}")
    
    def StatusDaq(self, parameters):
        """
        Docstring for StatusDaq
        
        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * check_hashpipe_running(bool) - check if hashpipe is running
                    * check_disk_usage(bool) - check the disk usage
                    * check_run_dirs(bool) - check the run dirs on daq node
        """
        # TODO: check if all of the parameters are reasonable
        #       We could use Pydantic for this. 
        request = daq_control_pb2.StatusDaqRequest()
        request.data_dir = parameters['data_dir']
        request.check_hashpipe_running = parameters['check_hashpipe_running']
        request.check_disk_usage = parameters['check_disk_usage']
        request.check_run_dirs = parameters['check_run_dirs']
        try:
            resp = self.stub.StatusDaq(request)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
            status = {}
            status['hashpipe_running'] = resp.hashpipe_running
            status['disk_usage'] = resp.disk_usage
            status['run_dirs'] = resp.run_dirs
            return resp.success, status
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}")
    
    def CleanupData(self, parameters):
        """
        Docstring for StatusDaq
        
        :param parameters: A dict contains all necessary parameters
                    * data_dir(str) - root dir for PANOSETI data
                    * run_dir(str) - the new directory for this run, 
                                    which should be under data_dir
                    * module_ids (list) - modules for this daq node
        """
        request = daq_control_pb2.CleanupDataRequest()
        request.data_dir = parameters['data_dir']
        request.run_dir = parameters['run_dir']
        request.module_id.extend(parameters['module_id'])
        try:
            resp = self.stub.CleanupData(request)
            if not resp.success:
                raise ValueError(f"Server rejected data: {resp.message}")
            return resp.success
        except grpc.RpcError as e:
            raise ConnectionError(f"gRPC failed: {e.details()}")
