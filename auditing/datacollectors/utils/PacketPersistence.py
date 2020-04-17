import logging, os, time 
import json


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG if os.environ.get("ENVIRONMENT") == "DEV" else logging.INFO)

logging.getLogger("pika").setLevel(logging.WARNING)

# Uncommenting these lines will allow us to write to DB directly
# import auditing.db.Service as db_service
# def save(pkt):
#   db_service.save(pkt)

import pika

channel = None

# key= data_collector id. value= channel
channel_per_data_collector = {}


def save(packet_writter_message, collector_id='default'):
    if len(packet_writter_message['messages']) == 0:
        LOG.error("Received a MQ message from Collector ID {0} without messages: {1}".format(collector_id,
                                                                                             packet_writter_message))
        return

    try:
        # Add timestamp to packet_writter_message
        packet_writter_message['ts'] = int(time.time())

        global channel_per_data_collector
        if collector_id not in channel_per_data_collector or channel_per_data_collector[
            collector_id].connection.is_closed:
            rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"],
                                                       os.environ["RABBITMQ_DEFAULT_PASS"])

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]),
                                          credentials=rabbit_credentials)
            )

            channel_per_data_collector[collector_id] = connection.channel()
            channel_per_data_collector[collector_id].queue_declare(queue='collectors_queue', durable=True)

        channel_per_data_collector[collector_id].basic_publish(
            exchange='',
            routing_key='collectors_queue',
            body=json.dumps(packet_writter_message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
    except Exception as e:
        LOG.error(
            "Error when connecting to data-collectors packet queue: " + str(e) + "Collector ID: " + str(collector_id))


def close_connection(collector_id):
    if collector_id in channel_per_data_collector:
        channel_per_data_collector[collector_id].close()
        del channel_per_data_collector[collector_id]


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
