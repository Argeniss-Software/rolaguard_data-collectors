import pika
import logging
import os
import json
import queue

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG if os.environ.get(
    "ENVIRONMENT") == "DEV" else logging.INFO)

logging.getLogger("pika").setLevel(logging.WARNING)


class Publisher(object):
    """Publisher that will handle unexpected interactions with 
    RabbitMQ such as channel and connection closures.

    Pika connections are not thread safe, so if Publisher.run()
    is used inside a thread, only add_message_to_queue() and 
    schedule_stop() methods should be called outside of that 
    thread.
    """

    def __init__(self, collector_id, routing_key, queue_name, retry_interval=5):
        self.connection = None
        self.channel = None
        self.message_number = None
        self.stopping = False
        self.scheduled_stop = False
        self.collector_id = collector_id
        self.routing_key = routing_key
        self.queue_name = queue_name
        self.retry_interval = retry_interval
        self.message_queue = queue.Queue()

    def connect(self):
        rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"],
                                                   os.environ["RABBITMQ_DEFAULT_PASS"])

        return pika.SelectConnection(
            pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"],
                                      port=int(os.environ["RABBITMQ_PORT"]),
                                      credentials=rabbit_credentials),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def on_channel_closed(self, channel, reason):
        if 'Normal shutdown' in str(reason):
            pass
        else:
            LOG.warning(
                f'data-collectors packet queue, channel closed: {reason}. Collector ID: {str(self.collector_id)}')
        self.channel = None
        if not self.stopping:
            self.connection.close()

    def on_connection_open_error(self, _unused_connection, err):
        LOG.error(
            f'Connection to data-collectors packet queue failed, reopening in {str(self.retry_interval)} seconds: {err}. Collector ID: {str(self.collector_id)}')
        self.connection.ioloop.call_later(
            self.retry_interval, self.connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        self.channel = None
        if self.stopping:
            self.connection.ioloop.stop()
        else:
            LOG.warning(
                f'Connection to data-collectors packet queue was closed, reopening in {str(self.retry_interval)} seconds: {reason}. Collector ID: {str(self.collector_id)}')
            self.connection.ioloop.call_later(
                self.retry_interval, self.connection.ioloop.stop)

    def run(self):
        while not self.stopping:
            self.connection = None
            self.message_number = 0

            try:
                self.connection = self.connect()
                self.connection.ioloop.call_later(1, self.check_messages)
                self.connection.ioloop.call_later(1, self.check_stop)
                self.connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self.connection is not None and
                        not self.connection.is_closed):
                    # Finish closing
                    self.connection.ioloop.start()

    def check_messages(self):
        while not self.message_queue.empty():
            message = self.message_queue.get_nowait()
            self.publish_message(message)
        if not self.stopping:
            self.connection.ioloop.call_later(1, self.check_messages)

    def check_stop(self):
        if self.scheduled_stop:
            self.stop()
        else:
            self.connection.ioloop.call_later(1, self.check_stop)

    def publish_message(self, message):
        if self.channel is None or not self.channel.is_open:
            return

        self.channel.basic_publish(
            exchange='',
            routing_key=self.routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
        self.message_number += 1

    def add_message_to_queue(self, message):
        self.message_queue.put(message)

    def schedule_stop(self):
        self.scheduled_stop = True

    def stop(self):
        self.stopping = True
        if self.channel is not None:
            self.channel.close()
        if self.connection is not None:
            self.connection.close()
