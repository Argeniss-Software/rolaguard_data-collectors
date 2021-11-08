import logging, os, time 
import json
import threading
from auditing.datacollectors.utils.Publisher import Publisher


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG if os.environ.get("ENVIRONMENT") == "DEV" else logging.INFO)

logging.getLogger("pika").setLevel(logging.WARNING)

# Uncommenting these lines will allow us to write to DB directly
# import auditing.db.Service as db_service
# def save(pkt):
#   db_service.save(pkt)

import pika

channel = None

# key= data_collector id. value= publisher
data_collector_publishers = {}

def run_publisher_thread(publisher):
    publisher.run()

def save(packet_writter_message, collector_id='default'):
    if len(packet_writter_message['messages']) == 0:
        LOG.error("Received a MQ message from Collector ID {0} without messages: {1}".format(collector_id,
                                                                                             packet_writter_message))
        return

    try:
        # Add timestamp to packet_writter_message
        packet_writter_message['ts'] = int(time.time())

        global data_collector_publishers
        if collector_id not in data_collector_publishers:
            publisher = Publisher(collector_id=collector_id,
                                  routing_key='collectors_queue',
                                  queue_name='collectors_queue')

            publisher_thread = threading.Thread(
                target=run_publisher_thread, args=(publisher,))
            publisher_thread.daemon = True
            publisher_thread.start()

            data_collector_publishers[collector_id] = publisher

        data_collector_publishers[collector_id].add_message_to_queue(packet_writter_message)
    except Exception as e:
        LOG.error(
            "Error when connecting to data-collectors packet queue: " + str(e) + "Collector ID: " + str(collector_id))


def close_connection(collector_id):
    if collector_id in data_collector_publishers:
        data_collector_publishers[collector_id].schedule_stop()
        time.sleep(2)
        del data_collector_publishers[collector_id]


def save_parsing_error(collector_id, message):
    notify_status_event(collector_id, {'data_collector_id': collector_id, 'message': message, 'type': 'FAILED_PARSING'})


def save_login_error(collector_id):
    event = {'data_collector_id': collector_id, 'type': 'FAILED_LOGIN'}
    LOG.error("Error when trying to login to TTN server. Data collector id: " + str(collector_id))
    notify_status_event(collector_id, event)


def notify_status_event(collector_id, event):
    try:
        rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"],
                                                   os.environ["RABBITMQ_DEFAULT_PASS"])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]),
                                      credentials=rabbit_credentials))
        channel = connection.channel()
        channel.queue_declare(queue='data_collectors_status_events')
        channel.basic_publish(exchange='', routing_key='data_collectors_status_events',
                              body=json.dumps(event).encode('utf-8'))

        connection.close()

    except Exception as e:
        LOG.error("Error when sending parsing error to queue: " + str(e) + "Collector ID: " + str(collector_id))


def notify_test_event(collector_id, event, result_message=''):
    try:
        message = {'data_collector_id': collector_id, 'type': event, 'message': result_message}

        LOG.info(f"Test result: {message}")

        rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"],
                                                   os.environ["RABBITMQ_DEFAULT_PASS"])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]),
                                      credentials=rabbit_credentials))
        channel = connection.channel()
        channel.queue_declare(queue='data_collectors_test_events')
        channel.basic_publish(exchange='', routing_key='data_collectors_test_events',
                              body=json.dumps(message).encode('utf-8'))
        connection.close()
    except Exception as e:
        LOG.error("Error when test notification to queue: " + str(e) + "Collector ID: " + str(collector_id))
