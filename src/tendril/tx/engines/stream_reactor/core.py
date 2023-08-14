

import json
import uuid

from twisted.application import service
from twisted.internet.task import LoopingCall
from twisted.internet.defer import DeferredQueue
from twisted.internet.defer import inlineCallbacks

from tendril.tx.utils.logger import TwistedLoggerMixin

from tendril.common.streams.exceptions import MangledMessageError
from tendril.common.streams.exceptions import JSONParseError

from tendril.tx.engines.stream_reactor.channel import ProcessingChannel
from tendril.tx.engines.stream_reactor.metrics import StreamReactorMetricsManager

from tendril.config import MQ_SERVER_EXCHANGE


class StreamReactor(service.Service, TwistedLoggerMixin):
    name = "stream-reactor"

    def __init__(self, mq_bind,
                 mq_exchange_name=MQ_SERVER_EXCHANGE,
                 mq_service_name='amqp091'):
        super(StreamReactor, self).__init__()
        self.log_init()
        self._mq_service_name = mq_service_name
        self._mq_exchange_name = mq_exchange_name
        self._mq_bind = mq_bind
        self.mq = None
        self._node_id = str(uuid.uuid1())

        self._datatypes = {}
        self._channels = {}

        self._monitor_task = None
        self._metrics = StreamReactorMetricsManager(self.log)

        self.received_messages = DeferredQueue(size=1000, backlog=1)
        self.reshaped_messages = DeferredQueue(size=1000, backlog=1)
        self.transit_messages = DeferredQueue(size=1000, backlog=1)

        self._handlers = []

    def register_callback(self, handler):
        self._handlers.append(handler)

    def startService(self):
        mq_service = self.parent.getServiceNamed(self._mq_service_name)  # pylint: disable=E1111,E1121
        self.mq = mq_service.getFactory()

        self.log.info(f"Creating queue {self._node_id} "
                      f"with binding {self._mq_bind} "
                      f"to {self._mq_exchange_name}")

        self.mq.read_messages(self._mq_exchange_name, self._mq_bind,
                              self.collect, queue=self._node_id)

        self._start_engine_tasks()
        self._start_monitoring_tasks()
        self._start_application_tasks()

    def _start_application_tasks(self):
        pass

    def _start_engine_tasks(self):
        self.log.info("Starting Engine Tasks")
        self._reshape()
        self._process()
        self._disseminate()
        self.log.info("Done Starting Engine Tasks")

    def _start_monitoring_tasks(self):
        self.log.info("Starting Monitoring Tasks")
        self.monitor_task.start(1)
        self.log.info("Done Starting Monitoring Tasks")

    def _monitor(self):
        self._metrics.current.l_rm = len(self.received_messages.pending)
        self._metrics.current.l_rsm = len(self.reshaped_messages.pending)
        self._metrics.current.l_gm = len(self.transit_messages.pending)
        self._metrics.capture()

    @property
    def monitor_task(self):
        if self._monitor_task is None:
            self.log.info("Creating metrics monitoring task")
            self._monitor_task = LoopingCall(self._monitor)
        return self._monitor_task

    def collect(self, message):
        self._metrics.current.c_rx += 1
        self.received_messages.put(message)

    def reshape(self, msg):
        return msg

    @inlineCallbacks
    def _reshape(self):
        self.log.info("Starting Reshaper Loop")
        while True:
            msg = yield self.received_messages.get()
            msg = msg.body
            try:
                try:
                    received_dict = json.loads(msg)
                except json.JSONDecodeError:
                    raise JSONParseError(msg)

                message_reshaped = self.reshape(received_dict)
            except MangledMessageError as e:
                self.log.warn("Error parsing received datapoint:\n{parse_err}",
                              parse_err=repr(e))
                self._metrics.current.c_rs_f += 1
                continue

            if message_reshaped:
                self._metrics.current.c_rs += 1
                self.reshaped_messages.put(message_reshaped)
            else:
                pass

    def get_channel_id(self, message):
        raise NotImplementedError

    def get_channel_processors(self, message):
        return []

    def _get_channel(self, message):
        _channel_id = self.get_channel_id(message)
        if _channel_id not in self._channels.keys():
            channel = ProcessingChannel(_channel_id)
            for processor in self.get_channel_processors(message):
                channel.install_processor(processor)
            self._channels[_channel_id] = channel
        return self._channels[_channel_id]

    @inlineCallbacks
    def _process(self):
        self.log.info("Starting Processor Loop")
        while True:
            _message = yield self.reshaped_messages.get()
            _channel = self._get_channel(_message)
            response = _channel.process(_message)
            self._metrics.current.c_proc += 1
            if response:
                self.transit_messages.put(response)

    @inlineCallbacks
    def _disseminate(self):
        self.log.info("Starting Dissemination Loop")
        while True:
            generated_message = yield self.transit_messages.get()
            self._metrics.current.c_tx += 1
            self.log.debug("Generated Message : {msg}", msg=generated_message)
            for handler in self._handlers:
                handler(generated_message)
