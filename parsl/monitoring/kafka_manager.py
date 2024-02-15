from collections import defaultdict
import socket
import logging
from typing import NamedTuple, Any, Dict

from parsl.monitoring.message_type import MessageType

try:
    from diaspora_event_sdk import KafkaProducer
except ImportError:
    _kafka_enabled = False
else:
    _kafka_enabled = True
    logger.warning("Writing Monitoring Messages to Kafka")

logger = logging.getLogger(__name__)

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


DEFAULT_MESSAGE_MAPPING = {
    MessageType.TASK_INFO: "green-faas-predictions",
    MessageType.RESOURCE_INFO: "green-faas-resources",
    MessageType.ENERGY_INFO: "green-faas-resources"
}


class KafkaConfig(NamedTuple):
    topic: dict[MessageType, str] | str = DEFAULT_MESSAGE_MAPPING

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (dataetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

class KafkaManager:

    if not _kafka_enabled:
        raise OptionalModuleMissing(['diaspora_event_sdk'],
                                    ("Kafka requires the diaspora event sdk"))

    def __init__(self,
                 config: KafkaConfig,
                 logdir: str, = '.'
                 logging_level: int = logging.INFO
                 ):

        self.producer = KafkaProducer(value_serializer=KafkaManager.serialize)
        if isinstance(config.topic, dict):
            self.topic_map = config.topic
        elif isinstance(config.topic, str):
            self.topic_map = defaultdict(config.topic)
        else:
            raise ValueError("Topic contains an invalid value")
            
    def start(self, message_queue):
        self.pending_sends = []

        while True:
            try:
                x, addr = message_queue.get(timeout=0.1)
            except queue.Empty:
                for send_future in futures.as_completed(self.pending_sends):
                    if send_future.exception():
                        logger.warning("Exception occurred during message send: %s", send_future.exception())
                    self.pending_sends = []
                continue
            else:
                if x == 'STOP':
                    break
                else:
                    self.pending_sends.append(self.send(x))
            
            if self.pending_sends > 1000:
                for send_future in futures.as_completed(self.pending_sends):
                    if send_future.exception():
                        logger.warning("Exception occurred during message send: %s", send_future.exception())
                    self.pending_sends = []
        
        for send_future in futures.as_completed(self.pending_sends):
            if send_future.exception():
                logger.warning("Exception occurred during message send: %s", send_future.exception())
            self.pending_sends = []

    def send(self, message):
        topic = self.topic_map[message[0]]
        key = message[1]["run_id"] # So we can partition streams based on run
        self.producer.send(topic=topic, key=key, value=message)

    @staticmethod
    def serialize(value):
        return json.dumps(v, cls=DateTimeEncoder).encode("utf-8")


@wrap_with_logs(target="kafka_manager")
def kafka_starter(exception_q: "queue.Queue[Tuple[str, str]]",
                  message_q: "queue.Queue[AddressedMonitoringMessage]",
                  kafka_config: KafkaConfig,
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
        dbm.start(message_q)
    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt signal caught")
        dbm.close()
        raise
    except Exception as e:
        logger.exception("kafka_manager.start exception")
        exception_q.put(("DBM", str(e)))
        dbm.close()

    logger.info("End of kafka_starter")