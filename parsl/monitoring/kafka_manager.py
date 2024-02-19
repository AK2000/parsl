from collections import defaultdict
import concurrent.futures
import datetime
import socket
import logging
import json
from typing import NamedTuple, Any, Dict, Optional, Union
from multiprocessing import Queue
import queue

from parsl.monitoring.message_type import MessageType
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

logger = logging.getLogger(__name__)

try:
    from diaspora_event_sdk import KafkaProducer
except ImportError:
    _kafka_enabled = False
else:
    _kafka_enabled = True
    logger.warning("Writing Monitoring Messages to Kafka")

def start_file_logger(filename: str, name: str = 'kafka', level: int = logging.DEBUG, format_string: Optional[str] = None) -> logging.Logger:
    """Add a stream log handler.

    Parameters
    ---------

    filename: string
        Name of the file to write logs to. Required.
    name: string
        Logger name.
    level: logging.LEVEL
        Set the logging level. Default=logging.DEBUG
        - format_string (string): Set the format string
    format_string: string
        Format string to use.

    Returns
    -------
        None.
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

class KafkaManager:

    if not _kafka_enabled:
        raise OptionalModuleMissing(['diaspora_event_sdk'],
                                    ("Kafka requires the diaspora event sdk"))

    def __init__(self,
                 topic_map: Union[str, dict],
                 logdir: str = '.',
                 logging_level: int = logging.INFO,
                 ):

        self.producer = KafkaProducer(value_serializer=KafkaManager.serialize)
        if isinstance(topic_map, dict):
            self.topic_map = topic_map
        elif isinstance(topic_map, str):
            self.topic_map = defaultdict(lambda : topic_map)
        else:
            raise ValueError("Topic contains an invalid value")

    def start(self, message_queue):
        self.pending_sends = []
        logger.info("Starting Kafka manager")

        while True:
            try:
                x, addr = message_queue.get(timeout=0.1)
            except queue.Empty:
                pass
            else:
                if x == 'STOP':
                    logger.info(f"Recieved stop signal from monitoring hub")
                    break
                else:
                    f = self.send(x)
                    if f is not None:
                        self.pending_sends.append(f)
            
            if len(self.pending_sends) > 10000:
                for send_future in self.pending_sends:
                    if send_future.exception:
                        logger.warning("Exception occurred during message send: %s", send_future.exception())
                    self.pending_sends = []

        logger.info("Draining Kafka manager")

        self.producer.flush()
        for send_future in self.pending_sends:
            if send_future.exception:
                logger.warning("Exception occurred during message send: %s", send_future.exception())
            self.pending_sends = []
        
        logger.info("Kafka manager exiting")

    def send(self, message):
        # Hacky way to deal with message from function wrapper 
        # TODO: Change me after fixing wrapper
        msg_type = message[0]
        if msg_type == MessageType.RESOURCE_INFO and (message[1]["first_msg"] or message[1]["last_msg"]):
            msg_type = MessageType.TASK_INFO
        try:
            topic = self.topic_map[msg_type]
        except:
            logger.info(f"Ignoring message of type {msg_type} that was not sent to a topic")
            return None
        else:
            key = message[1]["run_id"].encode("utf-8") # So we can partition streams based on run
            message[1]["msg_type"] = msg_type.name
            logger.info(f"Sending message of type {key}:{msg_type} to topic {topic}")
            future = self.producer.send(topic=topic, key=key, value=message[1])
            logger.info(f"Sent message")
            return future

    @staticmethod
    def serialize(value):
        return json.dumps(value, cls=DateTimeEncoder).encode("utf-8")


@wrap_with_logs(target="kafka_manager")
def kafka_starter(exception_q: "queue.Queue[Tuple[str, str]]",
                  message_q: "queue.Queue[AddressedMonitoringMessage]",
                  kafka_config: Union[str, dict],
                  logdir: str,
                  logging_level: int) -> None:
    """Start the kafka manager process

    The DFK should start this function. The args, kwargs match that of the monitoring config

    """
    setproctitle("parsl: kafka manager")

    global logger
    logger = start_file_logger("{}/kafka_manager.log".format(logdir),
                                name="kafka_manager",
                                level=logging_level)

    try:
        kafka_manager = KafkaManager(kafka_config)
        logger.info("Starting kafka manager in kafka starter")
        kafka_manager.start(message_q)
    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt signal caught")
        raise
    except Exception as e:
        logger.exception("kafka_manager.start exception")
        exception_q.put(("Kafka", str(e)))

    logger.info("End of kafka_starter")