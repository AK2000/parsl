import argparse
import logging
import json
import queue
import time
import threading
import uuid
import os

from parsl.process_loggers import wrap_with_logs

from parsl.monitoring.energy.base import NodeEnergyMonitor, Result
import parsl.monitoring.energy.node_monitors as NodeEnergyMonitors
from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios import MonitoringRadio, UDPRadio, HTEXRadio, FilesystemRadio


def start_file_logger(filename, rank, name=__name__, level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d " \
                        "%(process)d %(threadName)s " \
                        "[%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def prepare_htex_radio(uid,
                       kill_event, 
                       addresses="127.0.0.1",
                       result_port="50098",
                       address_probe_timeout=30,
                       poll_period=10,
                       max_queue_size=10):
    import zmq
    from parsl.executors.high_throughput.probe import probe_addresses

    try:
        ix_address = probe_addresses(addresses.split(','), result_port, timeout=address_probe_timeout)
        if not ix_address:
            raise Exception("No viable address found")
    except:
        logger.exception("Caught exception while trying to determine viable address to interchange")
        print("Failed to find a viable address to connect to interchange. Exiting")
        exit(5)
    
    result_q_url = "tcp://{}:{}".format(ix_address, result_port)
    logger.info("Result url : {}".format(result_q_url))

    context = zmq.Context()
    result_outgoing = context.socket(zmq.DEALER)
    result_outgoing.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
    result_outgoing.setsockopt(zmq.LINGER, 0)
    result_outgoing.connect(result_q_url)
    logger.info("Monitor connected to interchange")

    @wrap_with_logs
    def push_results(pending_results_queue, result_outgoing, poll_period, max_queue_size, kill_event):
        """ Listens on the pending_result_queue and sends out results via zmq

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """

        logger.debug("Starting result push thread")

        push_poll_period = max(10, poll_period) / 1000    # push_poll_period must be atleast 10 ms
        logger.debug("push poll period: {}".format(push_poll_period))

        last_beat = time.time()
        last_result_beat = time.time()
        items = []

        while not kill_event.is_set():
            try:
                logger.debug("Starting pending_result_queue get")
                r = pending_result_queue.get(block=True, timeout=push_poll_period)
                logger.debug("Got a result item")
                items.append(r)
            except queue.Empty:
                logger.debug("pending_result_queue get timeout without result item")
            except Exception as e:
                logger.exception("Got an exception: {}".format(e))

            if len(items) >= max_queue_size or time.time() > last_beat + push_poll_period:
                last_beat = time.time()
                if items:
                    logger.debug(f"Result send: Pushing {len(items)} items")
                    result_outgoing.send_multipart(items)
                    logger.debug("Result send: Pushed")
                    items = []
                else:
                    logger.debug("Result send: No items to push")
            else:
                logger.debug(f"Result send: check condition not met - deferring {len(items)} result items")

        logger.critical("Exiting")

    
    pending_result_queue = queue.Queue()
    result_pusher_thread = threading.Thread(target=push_results,
                                            args=(pending_result_queue, 
                                                  result_outgoing,
                                                  poll_period,
                                                  max_queue_size,
                                                  kill_event),
                                            name="Result-Pusher")
    result_pusher_thread.start()

    import parsl.executors.high_throughput.monitoring_info as mi
    mi.result_queue = pending_result_queue

def run(energy_monitor: NodeEnergyMonitor,
        monitoring_hub_url: str,
        block_id: str,
        run_id: str,
        radio_mode: str,
        logging_level: int,
        sleep_dur: float,
        run_dir: str,
        manager_id: str,
        log_only: bool = False) -> None:
    
    import logging
    import platform

    from parsl.utils import setproctitle

    logger = logging.getLogger(__name__)

    setproctitle("parsl: block energy monitor")

    if not log_only:
        radio: MonitoringRadio
        if radio_mode == "udp":
            radio = UDPRadio(monitoring_hub_url,
                            source_id=manager_id)
        elif radio_mode == "htex":
            radio = HTEXRadio(monitoring_hub_url,
                            source_id=manager_id)
        elif radio_mode == "filesystem":
            radio = FilesystemRadio(monitoring_url=monitoring_hub_url,
                                    source_id=manager_id, run_dir=run_dir)
        else:
            raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    logger.debug("start of energy monitor")

    def measure_and_prepare():
        d = energy_monitor.report().dict()
        d["devices"] = json.dumps(d["devices"])
        d["run_id"] = run_id
        d["block_id"] = block_id
        d['resource_monitoring_interval'] = sleep_dur
        d['hostname'] = platform.node()
        return d

    next_send = time.time()
    accumulate_dur = 5.0  # TODO: make configurable?

    while True:
        logger.info("start of monitoring loop")
        try:
            if time.time() >= next_send:
                d = measure_and_prepare()
                logger.info("Sending intermediate energy message")
                if not log_only:
                    radio.send((MessageType.ENERGY_INFO, d))
                else:
                    logger.info("Recorded Energy information\n"\
                                + "\n".join([f"\t{k}: {v}" for k,v in d.items()]))
                next_send += sleep_dur
        except Exception:
            logger.exception("Exception getting the energy usage. Not sending usage to Hub", exc_info=True)
        logger.debug("sleeping")

        # wait either until approx next send time, or the accumulation period
        # so the accumulation period will not be completely precise.
        # but before this, the sleep period was also not completely precise.
        # with a minimum floor of 0 to not upset wait

        #terminate_event.wait(max(0, min(next_send - time.time(), accumulate_dur)))
        time.sleep(max(0, min(next_send - time.time(), accumulate_dur)))

    logger.debug("Sending final energy message")
    try:
        d = measure_and_prepare()
        if not log_only:
            radio.send((MessageType.ENERGY_INFO, d))
    except Exception:
        logger.exception("Exception getting the energy usage. Not sending final usage to Hub", exc_info=True)
    logger.debug("End of monitoring helper")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--monitor",
                        help="Which monitor to use to read energy values") #TODO: Allow more clomplex interface
    parser.add_argument("-u", "--url", help="Monitoring hub url")
    parser.add_argument("-i", "--run_id", help="Run id")
    parser.add_argument("-b", "--block_id", default=None,
                        help="Block identifier for Manager")
    parser.add_argument("-r", "--radio_mode", choices=["udp", "htex", "filesystem"],
                        help="Which radio to use to communicate with monitoring hub")
    parser.add_argument("-l", "--logdir", default="process_energy_monitor_logs",
                        help="Process worker pool log directory")
    parser.add_argument("-d", "--debug", action="store_true", help="Create debug log messages")
    parser.add_argument("-s", "--sleep_dur", type=int, help="Sleep time in between monitoring")
    parser.add_argument("--rundir", help="Place to create file system radio info")
    parser.add_argument("-a", "--addresses", default='',
                        help="Comma separated list of addresses at which the interchange could be reached")
    parser.add_argument("--address_probe_timeout", default=30,
                        help="Timeout to probe for viable address to interchange. Default: 30s")
    parser.add_argument("--poll", default=10, type=int,
                        help="Poll period used in milliseconds")
    parser.add_argument("--result_port",
                        help="Result port for posting results to the interchange")
    parser.add_argument("--log_only", action="store_true", help="Only write logs, do not send data to monitoring DB")
    parser.add_argument("--uid", default=str(uuid.uuid4()).split('-')[-1],
                        help="Unique identifier string for monitor")
    parser.add_argument("--manager_id",
                        help="Unique identifier string for Manager")
    args = parser.parse_args()

    os.makedirs(os.path.join(args.logdir, "block-{}".format(args.block_id)), exist_ok=True)

    try:
        logger = start_file_logger('{}/block-{}/energy.log'.format(args.logdir, args.block_id),
                                   0,
                                   level=logging.DEBUG if args.debug is True else logging.INFO)
        logger.info("Debug logging: {}".format(args.debug))
        logger.info("Log dir: {}".format(args.logdir))
        logger.info("Block ID: {}".format(args.block_id))
        logger.info("Node Energy Monitor: {}".format(args.monitor))
        logger.info("Radio Mode: {}".format(args.radio_mode))
        logger.info("Monitoring Hub URL: {}".format(args.url))

        monitor_cls = getattr(NodeEnergyMonitors, args.monitor)
        monitor = monitor_cls(debug=args.debug)

        if args.radio_mode == "htex" and not args.log_only:
            kill_event = threading.Event()
            prepare_htex_radio(args.uid,
                               kill_event,
                               args.addresses,
                               args.result_port,
                               args.address_probe_timeout,
                               args.poll)

        run(monitor,
            monitoring_hub_url=args.url,
            block_id=args.block_id,
            run_id=args.run_id,
            radio_mode=args.radio_mode,
            logging_level=logging.DEBUG if args.debug is True else logging.INFO,
            sleep_dur=args.sleep_dur,
            run_dir=args.rundir,
            manager_id=args.manager_id,
            log_only=args.log_only)

    except Exception:
        logger.critical("Process energy monitor exiting with an exception", exc_info=True)
        if args.radio_mode == "htex" and not args.log_only:
            kill_event.set()
        raise
    else:
        logger.info("Process energy monitor exiting normally")
        print("Process energy monitor exiting normally")
        if args.radio_mode == "htex" and not args.log_only:
            kill_event.set()
