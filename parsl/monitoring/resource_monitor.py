import os
import time
import logging
import datetime
import logging
import platform
import psutil
import getpass
import performance_features
from functools import wraps

from parsl.multiprocessing import ForkProcess
from multiprocessing import Event, Barrier
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios import MonitoringRadio, UDPRadio, HTEXRadio, FilesystemRadio
from typing import Any, Callable, Dict, List, Sequence, Tuple

# these values are simple to log. Other information is available in special formats such as memory below.
simple = ["cpu_num", 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
# values that can be summed up to see total resources used by task process and its children
summable_values = ['memory_percent', 'num_threads']
# perfomance counters read from performance features that can be used to monitor energy
events= [['UNHALTED_CORE_CYCLES'], ['UNHALTED_REFERENCE_CYCLES'], ['LLC_MISSES'], ['INSTRUCTION_RETIRED']]

logger = logging.getLogger(__name__)

def accumulate_and_prepare(run_id: str,
                           proc: psutil.Process, 
                           profiler: performance_features.Profiler):

    children_user_time = {}  # type: Dict[int, float]
    children_system_time = {}  # type: Dict[int, float]

    event_counters = profiler.read_events()
    event_counters = profiler._Profiler__format_data([event_counters,])

    d = {"psutil_process_" + str(k): v for k, v in proc.as_dict().items() if k in simple}
    d["run_id"] = run_id
    d["pid"] = proc.pid
    d['hostname'] = platform.node()
    d['first_msg'] = False
    d['last_msg'] = False
    d['timestamp'] = datetime.datetime.now()

    logging.debug("getting children")
    children = proc.children(recursive=True)
    logging.debug("got children")

    d["psutil_cpu_count"] = psutil.cpu_count()
    d['psutil_process_memory_virtual'] = proc.memory_info().vms
    d['psutil_process_memory_resident'] = proc.memory_info().rss
    d['psutil_process_time_user'] = proc.cpu_times().user
    d['psutil_process_time_system'] = proc.cpu_times().system
    d['psutil_process_children_count'] = len(children)
    try:
        d['psutil_process_disk_write'] = proc.io_counters().write_chars
        d['psutil_process_disk_read'] = proc.io_counters().read_chars
    except Exception:
        # occasionally pid temp files that hold this information are unvailable to be read so set to zero
        logging.exception("Exception reading IO counters for main process. Recorded IO usage may be incomplete", exc_info=True)
        d['psutil_process_disk_write'] = 0
        d['psutil_process_disk_read'] = 0
    for child in children:
        for k, v in child.as_dict(attrs=summable_values).items():
            d['psutil_process_' + str(k)] += v
        child_user_time = child.cpu_times().user
        child_system_time = child.cpu_times().system
        children_user_time[child.pid] = child_user_time
        children_system_time[child.pid] = child_system_time
        d['psutil_process_memory_virtual'] += child.memory_info().vms
        d['psutil_process_memory_resident'] += child.memory_info().rss
        try:
            d['psutil_process_disk_write'] += child.io_counters().write_chars
            d['psutil_process_disk_read'] += child.io_counters().read_chars
        except Exception:
            # occassionally pid temp files that hold this information are unvailable to be read so add zero
            logging.exception("Exception reading IO counters for child {k}. Recorded IO usage may be incomplete".format(k=k), exc_info=True)
            d['psutil_process_disk_write'] += 0
            d['psutil_process_disk_read'] += 0
    total_children_user_time = 0.0
    for child_pid in children_user_time:
        total_children_user_time += children_user_time[child_pid]
    total_children_system_time = 0.0
    for child_pid in children_system_time:
        total_children_system_time += children_system_time[child_pid]
    d['psutil_process_time_user'] += total_children_user_time
    d['psutil_process_time_system'] += total_children_system_time

    # Send event counters
    d['perf_unhalted_core_cycles'] = event_counters[0][0]
    d['perf_unhalted_reference_cycles'] = event_counters[0][1]
    d['perf_llc_misses'] = event_counters[0][2]
    d['perf_instructions_retired'] = event_counters[0][3]
    
    logging.debug("sending message")
    return d
    

@wrap_with_logs
def resource_monitor_loop(monitoring_hub_url: str,
            manager_id: str,
            run_id: str,
            radio_mode: str,
            logging_level: int,
            sleep_dur: float,
            run_dir: str,
            terminate_event: Any) -> None:  # cannot be Event/Barrier because of multiprocessing type weirdness.
    """Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.

    This process makes calls to logging, but deliberately does not attach
    any log handlers. Previously, there was a handler which logged to a
    file in /tmp, but this was usually not useful or even accessible.
    In some circumstances, it might be useful to hack in a handler so the
    logger calls remain in place.
    """

    setproctitle("parsl: resource monitor")

    radio: MonitoringRadio
    if radio_mode == "udp":
        radio = UDPRadio(monitoring_hub_url,
                         source_id=task_id)
    elif radio_mode == "htex":
        radio = HTEXRadio(monitoring_hub_url,
                          source_id=task_id)
    elif radio_mode == "filesystem":
        radio = FilesystemRadio(monitoring_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    logging.debug("start of monitor")

    user_name = getpass.getuser()

    profilers = dict()
    
    next_send = time.time()

    while not terminate_event.is_set():
        logging.debug("start of monitoring loop")
        for proc in psutil.process_iter(['pid', 'username']):
            if proc.info.username != user_name or proc.info.pid == os.getpid():
                continue

            if proc.info.pid not in profilers:
                profiler = performance_features.Profiler(pid=proc.info.pid, events_groups=events)
                profiler._Profiler__initialize()
                profiler.reset_events()
                profiler.enable_events()
                profiler.program.start()
                profilers[proc.info.pid] = profiler
            
            profiler = profilers[proc.info.pid]

            try:
                d = accumulate_and_prepare(run_id, proc, profiler)
                logging.debug("Sending intermediate resource message")
                radio.send((MessageType.RESOURCE_INFO, d))
                next_send += sleep_dur
            except Exception:
                logging.exception("Exception getting the resource usage. Not sending usage to Hub", exc_info=True)
        
        logging.debug("sleeping")
        terminate_event.wait(max(0, next_send - time.time()))