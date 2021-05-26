import http.client
import json
import time

import os
from auditing import iot_logging
import pika
from threading import Thread

from auditing.datacollectors.LoraServerIOCollector import LoraServerIOCollector
from auditing.datacollectors.TTNCollector import TTNCollector

from auditing.datacollectors.utils.PacketPersistence import close_connection

LOG = iot_logging.getLogger(__name__)

collectors_dict_connected = dict()
collectors_dict_verified = dict()

seconds_inactive = 4
collectors = []


def main():
    LOG.info('Starting Orchestrator')
    data_collectors = []

    try:
        data_collectors = fetch_data_collectors()
    except Exception as exc:
        LOG.error('Something went wrong fetching data collectors.\n' + str(exc))
        exit(-1)

    LOG.info(f"Found {len(data_collectors)} data collectors")
    rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]),
                                  credentials=rabbit_credentials))
    channel = connection.channel()

    for dc in data_collectors:
        for collector in create_collector(dc):
            if dc.get('status') != 'DISABLED':

                # Sending this event is confusing for the user because it shows a
                #  restart event in the logs when we deploy this code

                # event = {
                #     "data_collector_id": collector.data_collector_id,
                #     "status": 'DISCONNECTED',
                #     "is_restart": True
                # }
                # event = json.dumps(event)
                # try:
                #     channel.basic_publish(exchange='', routing_key='data_collectors_status_events',
                #                           body=event.encode('utf-8'))
                # except Exception as e:
                #     LOG.error("Error when sending status update to queue while starting collector: " + str(
                #         e) + "Collector ID: " + str(collector.data_collector_id))

                collector.connect()
            collectors.append(collector)
    connection.close()

    thread = Thread(target=check_data_collectors_status)
    thread.setDaemon(True)
    thread.start()

    consumer()


def consumer():
    rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]),
                                  credentials=rabbit_credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=os.environ["ENVIRONMENT"], exchange_type='direct')
    channel.queue_declare(queue='collectors_queue', durable=True)
    channel.queue_bind(exchange=os.environ["ENVIRONMENT"], queue='collectors_queue')
    channel.queue_declare(queue='data_collectors_events')
    channel.basic_consume(on_message_callback=handle_events, queue='data_collectors_events', auto_ack=True)
    channel.start_consuming()


def check_data_collectors_status():
    # Wait until every collector is up after deployment. This is to avoid unstable logging in the frontend.
    time.sleep(60*2)

    while (True):
        try:
            rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"],
                                                       os.environ["RABBITMQ_DEFAULT_PASS"])
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]),
                                          credentials=rabbit_credentials))
            channel = connection.channel()
            channel.queue_declare(queue='data_collectors_status_events')

            for collector in collectors:
                collector_id = collector.data_collector_id

                if collector.connected != collectors_dict_connected.get(collector_id, 'CONNECTED') or \
                        collector.verified != collectors_dict_verified.get(collector_id, False):

                    body = {
                        "data_collector_id": collector_id,
                        "status": collector.connected,
                        "verified": collector.verified
                    }
                    body = json.dumps(body).encode('utf-8')

                    try:
                        channel.basic_publish(exchange='', routing_key='data_collectors_status_events', body=body)

                    except Exception as e:
                        LOG.error("Error when sending status update to queue: " + str(e) + "Collector ID: " + str(
                            collector_id))

                    collectors_dict_connected[collector_id] = collector.connected
                    collectors_dict_verified[collector_id] = collector.verified

                if isinstance(collector, TTNCollector) and not collector.manually_disconnected and collector.ws \
                        and collector.ws.is_closed:
                    collector.connect()
                    LOG.info('Trying to reconnect gw: ' + collector.gateway_id)

            connection.close()

        except Exception as exc:
            LOG.error('Error in publish: ' + str(exc))

        time.sleep(5)


def handle_events(ch, method, properties, body):
    try:
        event = json.loads(body.decode('utf-8'))
    except Exception as exc:
        LOG.error("Couldn\'t deserialize event. Exception: {0}".format(exc))

        return

    data_collector_id = event.get('data').get('id')
    event_type = event.get('type')

    if event_type == 'CREATED':
        try:
            for collector in create_collector(event.get('data')):
                collector.connect()
                collectors.append(collector)
                collectors_dict_connected[data_collector_id] = 'DISCONNECTED'
                collectors_dict_verified[data_collector_id] = False

        except Exception as exc:
            LOG.error("Error when create new Collector. Exception: {0}".format(exc))

    elif event_type == 'DELETED':
        try:
            for collector in [c for c in collectors if c.data_collector_id == data_collector_id]:
                collector.disconnect()
                collectors.remove(collector)

            del collectors_dict_connected[data_collector_id]
            del collectors_dict_verified[data_collector_id]
            close_connection(data_collector_id)

        except Exception as exc:
            LOG.error("Error when delete new Collector. Exception: {0}".format(exc))

    elif event_type == 'ENABLED':
        try:
            for collector in [c for c in collectors if c.data_collector_id == data_collector_id]:
                collector.connect()
                collector.disabled = False
            collectors_dict_connected[data_collector_id] = 'DISCONNECTED'
        except Exception as exc:
            LOG.error("Error when enable new Collector. Exception: {0}".format(exc))

    elif event_type == 'DISABLED':
        disable_collector(data_collector_id)

    elif event_type == 'UPDATED':
        close_connection(data_collector_id)
        disabled = False

        for collector in  [c for c in collectors if c.data_collector_id == data_collector_id]:
            collector.disconnect()
            disabled = disabled or collector.disabled
            collectors.remove(collector)


        event = {
            "data_collector_id": data_collector_id,
            "status": 'DISCONNECTED'
        }
        event = json.dumps(event)

        try:
            rabbit_credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"],
                                                       os.environ["RABBITMQ_DEFAULT_PASS"])
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=os.environ["RABBITMQ_HOST"], port=int(os.environ["RABBITMQ_PORT"]),
                                          credentials=rabbit_credentials))
            channel = connection.channel()
            channel.basic_publish(exchange='', routing_key='data_collectors_status_events', body=event.encode('utf-8'))
            connection.close()
        except Exception as e:
            LOG.error(
                "Error when sending status update to queue in UPDATE event: " + str(e) + "Collector ID: " + str(
                    data_collector_id))

        for collector in create_collector(event.get('data')):
            if not disabled:
                collector.connect()
                collectors_dict_connected[data_collector_id] = 'DISCONNECTED'
            collectors.append(collector)

    elif event_type == 'TEST':
        try:
            for collector in create_collector(event.get('data')):
                collector.test()

        except Exception as exc:
            LOG.error("Error when testing Collector. Exception: %s" % (exc))

    elif event_type == 'FAILED_VERIFY':
        LOG.error(event.get('message'))
        disable_collector(data_collector_id)


def disable_collector(data_collector_id):
    try:
        for collector in [c for c in collectors if c.data_collector_id == data_collector_id]:
            collector.disconnect()
            collector.disabled = True
        del collectors_dict_connected[data_collector_id]
        del collectors_dict_verified[data_collector_id]
        close_connection(data_collector_id)
    except Exception as exc:
        LOG.error("Error when disable new Collector. Exception: {0}".format(exc))


def create_collector(dc):
    topics = list()

    if dc.get('topics', None) != None and len(dc.get('topics')) > 0:
        for topic in dc.get('topics'):
            topics.append((topic, 0))
    else:
        topics.append(('#', 0))

    type = dc.get('type').get('type')
    LOG.debug(f"Creating collector of type {type}")
    collectors = []

    if type == 'chirpstack_collector':
        collectors.append(
            LoraServerIOCollector(
                data_collector_id=dc.get('id'),
                organization_id=dc.get('organization_id'),
                host=dc.get('ip'),
                port=int(dc.get('port')),
                ssl=dc.get('ssl'),
                user=dc.get('user'),
                password=dc.get('password'),
                last_seen=dc.get('last_seen'),
                connected=dc.get('connected'),
                topics=topics,
                verified=dc.get('verified'),
                ca_cert=dc.get('ca_cert'),
                client_cert=dc.get('client_cert'),
                client_key=dc.get('client_key')
                )
        )
    elif type == 'ttn_collector':
        gateways = [gw.strip() for gw in dc['gateway_id'].split(",")]

        for gw in gateways:
            collectors.append(
                TTNCollector(
                    data_collector_id=dc.get('id'),
                    organization_id=dc.get('organization_id'),
                    user=dc.get('user'),
                    password=dc.get('password'),
                    gateway_id=gw,
                    verified=dc.get('verified')
                )
            )
    else:
        LOG.error('Unknown/unsupported Data Collector Type: {0}'.format(type))

    return collectors


def fetch_data_collectors():
    host = os.environ['API_HOST']
    user = os.environ['API_USER']
    password = os.environ['API_PASSWORD']
    conn = http.client.HTTPConnection(host)
    headers = {'Content-type': 'application/json'}
    credentials = {'username': user, 'password': password}
    conn.request('POST', '/api/v1.0/login', json.dumps(credentials), headers)
    response = conn.getresponse()
    parsed_response = json.loads(response.read().decode())
    token = parsed_response.get('access_token', None)

    if token:
        headers['Authorization'] = 'Bearer ' + token
        conn.request('GET', '/api/v1.0/data_collectors', None, headers)
        response = conn.getresponse()
        parsed_response = json.loads(response.read().decode())

        return parsed_response.get('data_collectors', None)
    else:
        raise ValueError(f'Could not login:\n{str(parsed_response)}')


main()
