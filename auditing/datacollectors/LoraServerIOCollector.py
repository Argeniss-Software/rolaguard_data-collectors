# LoRaWAN Security Framework - LoraServerIOCollector
# Copyright (c) 2019 IOActive Inc.  All rights reserved.

import sys
import argparse
import traceback
import json
import re
import time
import paho.mqtt.client as mqtt
import chirpstack_api.gw.gw_pb2 as api
import base64
import tempfile

from auditing import iot_logging
from time import sleep
from datetime import datetime, timedelta
import auditing.datacollectors.utils.PhyParser as phy_parser
from auditing.datacollectors.BaseCollector import BaseCollector
from auditing.datacollectors.utils.PacketPersistence import save, save_parsing_error, notify_test_event
from google.protobuf.json_format import MessageToJson



class LoraServerIOCollector(BaseCollector):
    TIMEOUT = 60

    """
    This collector establishes a connection to a MQTT broker. Thus, we're using the paho mqtt client.

    Once we are connected to the broker, the most important function is on_message(). There are two
    topics that provide the most valuable information: */gateway/* and */application/*.

    The topic */gateway/* provides the packets in B64 format (PHYPayload) and also TX/RX metadata. Nevertheless,
    there's rich metadata such as AppName, DeviceName, GatewayName, etc that's not in this topic. To fetch this
    data we need to read the topic */application/*.

    The topic */application/*, besides to providing the AppName, DeviceName, GatewayName, gives us the
    association between DevAddr and DevEUI.

    So, the 'packet' object sent to the DB could be formed with data from a gateway/* topic message and an
    application/* topic message (the later is a complement, not required). The gateway/* topic message will
    always generate a 'packet' object.

    For creating a packet, the MOST IMPORTANT steps in high-level are:
    1- We receive a message from a gateway/* topic message:
        a- If we have the metadata provided in application/* in memory, we can send the packet directly to the
        MQTT queue
        b- Otherwise, we store the prev_packet with the hope that an application/* message comes after
    2- After, we might receive an application/* topic message. In case we have a prev_packet, the DevAddr in
    both messages match, and the counter is the same (meaning both MQTT messages originated from the same message),
    we save this metadata and put it into the prev_packet which is then sent into the MQTT.

    ****Further considerations for the */gateway/* topic****
    Depending on how's configured the Chirsptack infrastructure, messages in the */gateway/* topic can have
    JSON or protobuf encoding.

    An observed rule is:
    * If message cannot be decoded to JSON and the topic ends in 'up', it's a protobuf message.
    * Otherwise, the message can be decoded to JSON and topic ends in 'rx' or 'tx'.

    If we receive a protobuf message, it usually comes with both rx and tx data. Otherwise, this data comes in
    separated messages. It might happend that in an 'up' message, we don't receive as many fields as in JSON
    messages.
    """

    def __init__(self, data_collector_id, organization_id, host, port, ssl, user, password, topics, last_seen,
                 connected, verified, ca_cert, client_cert, client_key):
        super().__init__(data_collector_id=data_collector_id, organization_id=organization_id, verified=verified)
        self.host = host
        self.port = port
        self.ssl = ssl
        self.user = user
        self.password = password
        self.topics = topics
        self.mqtt_client = None
        self.last_seen = None
        # This var saves half of the information (from the topic gateway/gw_id/rx) to be persisted
        self.prev_packet = None
        # The data sent to the MQTT queue, to be written by the packet writer. It must have at least one MQ message
        self.packet_writter_message = self.init_packet_writter_message()
        # This dict associates a dev_addr with a dict containing the {dev_eui, app_name and dev_name}
        self.devices_map = {}
        # Flag for testing the collector
        self.being_tested = False

        # client tls certificates for mqtt
        self.ca_cert = None
        self.client_cert = None
        self.client_key = None
        self.ca_cert_file = tempfile.NamedTemporaryFile()
        self.client_cert_file = tempfile.NamedTemporaryFile()
        self.client_key_file = tempfile.NamedTemporaryFile()

        if ca_cert is not None:
            self.ca_cert_file.write(ca_cert.encode('ascii'))
            self.ca_cert_file.flush()
            self.ca_cert = self.ca_cert_file.name

        if client_cert is not None:
            self.client_cert_file.write(client_cert.encode('ascii'))
            self.client_cert_file.flush()
            self.client_cert = self.client_cert_file.name

        if client_key is not None:
            self.client_key_file.write(client_key.encode('ascii'))
            self.client_key_file.flush()
            self.client_key = self.client_key_file.name


    def connect(self):
        super(LoraServerIOCollector, self).connect()

        if self.mqtt_client:
            print('Existing connection')
        else:
            self.mqtt_client = mqtt.Client()

            if self.ca_cert is not None:
                tls = {
                    'ca_certs': self.ca_cert,
                    'keyfile': self.client_key,
                    'certfile' :self.client_cert
                }
                self.mqtt_client.tls_set(**tls)
            self.mqtt_client.organization_id = self.organization_id
            self.mqtt_client.data_collector_id = self.data_collector_id
            self.mqtt_client.host = self.host
            self.mqtt_client.topics = self.topics
            self.mqtt_client.user_data_set(self)
            self.mqtt_client.on_connect = lambda client, userdata, flags, rc: self.on_connect(client, userdata, flags,
                                                                                              rc)
            self.mqtt_client.on_message = lambda client, userdata, msg: self.on_message(client, userdata, msg)
            self.mqtt_client.on_disconnect = lambda client, userdata, rc: self.on_disconnect(client, userdata, rc)
            self.mqtt_client.reconnect_delay_set(min_delay=10, max_delay=60)

            if self.password and self.user:
                self.log.info(f"Setting MQTT credentials for: {self.mqtt_client.host}")
                self.mqtt_client.username_pw_set(self.user, self.password)

            self.mqtt_client.connect_async(self.host, self.port, self.TIMEOUT)

            self.mqtt_client.prev_packet = self.prev_packet
            self.mqtt_client.packet_writter_message = self.packet_writter_message
            self.mqtt_client.devices_map = self.devices_map

            try:
                self.mqtt_client.loop_start()
            except KeyboardInterrupt:
                self.mqtt_client.disconnect()
                exit(0)

    def disconnect(self):
        if self.being_tested:
            self.log.info(f"Stopping test connection to: {self.mqtt_client.host if self.mqtt_client else 'N/A'}. "
                     f"DataCollector ID {self.data_collector_id}")
        else:
            self.log.info(f"Manually disconnected from: {self.mqtt_client.host if self.mqtt_client else 'N/A'}")

        if self.mqtt_client:
            self.mqtt_client.disconnect()
            self.mqtt_client = None
        self.ca_cert_file.close()
        self.client_cert_file.close()
        self.client_key_file.close()
        super(LoraServerIOCollector, self).disconnect()

    def reconnect(self):
        print('reconnection')

    def verify_topics(self, msg):
        if msg.topic[-5:] == "/join":
            return True

        if re.search('gateway/(.*)?/*', msg.topic) is not None:
            return True

        if re.search('application/.*?/device/(.*)/rx', msg.topic) is not None:
            return True

        if re.search('application/.*?/node/(.*)/rx', msg.topic) is not None:
            return True

        return False

    def verify_payload(self, msg):
        search = re.search('gateway/(.*)?/*', msg.topic)

        if search is None or (search.group(0)[-2:] not in ["rx", "tx"]):
            self.log.debug('topic does not include physical payload')

            return True  # NOT SURE if this should be True or False

        # try to decode payload as utf-8
        try:
            mqtt_messsage = json.loads(msg.payload.decode("utf-8"))
        except Exception as e:
            self.log.error(f'payload could not be decoded as utf8: {e}')

            return False

        # looks for phyPayload field in mqtt message
        phyPayload = mqtt_messsage.get('phyPayload', None)

        if not phyPayload:
            self.log.error('No phyPayload field in mqtt message')

            return False

        # PHYPayload shouldn't exceed 255 bytes by definition. In DB we support 300 bytes

        if len(phyPayload) > 300:
            return False

        # Parse the base64 PHYPayload
        try:
            phy_parser.setPHYPayload(phyPayload)

            return True
        except Exception as e:
            self.log.error(f'Error parsing physical payload: {e}')

            return False

    def on_message(self, client, userdata, msg):
        # We can receive both protobuf messages and JSON messages. We try to parse it as JSON and if it fails and the topic matches to /up, it's a protobuf instead
        is_protobuf_message= False

        if self.being_tested:
            return

        if not self.verified:
            if not self.verify_message(msg):
                self.log.debug("Collector is not yet verified, skipping message")

                return

        try:
            # print("Topic %s Packet %s"%(msg.topic, msg.payload))
            # If message cannot be decoded as json, skip it
            mqtt_messsage = json.loads(msg.payload.decode("utf-8"))

        except Exception as e:
            # First, check if we had a prev_packet. If so, first save it

            if client.prev_packet is not None:
                client.packet_writter_message['packet'] = client.prev_packet
                save(client.packet_writter_message, client.data_collector_id)
                # Reset vars
                client.packet_writter_message = self.init_packet_writter_message()
                client.prev_packet = None

            # If failed to decode message, then it's probably protobuf. To make sure, check the /up topic
            search = re.search('gateway/(.*)?/*', msg.topic)

            if search is not None and search.group(0)[-2:] in ["up"]:
                try:
                    uplink= api.UplinkFrame()
                    uplink.ParseFromString(msg.payload)
                    mqtt_messsage= json.loads(MessageToJson(uplink))
                    is_protobuf_message= True
                except Exception as e:
                    self.log.error(f'Error parsing protobuf: {e}. Protobuf message: {msg.payload}')
            else:
                # Save this message an topic into MQ
                client.packet_writter_message['messages'].append(
                    {
                        'topic': msg.topic,
                        'message': msg.payload.decode("utf-8"),
                        'data_collector_id': client.data_collector_id
                    }
                )
                save(client.packet_writter_message, client.data_collector_id)

                # Reset packet_writter_message
                client.packet_writter_message = self.init_packet_writter_message()

                save_parsing_error(collector_id=client.data_collector_id, message=str(e))

                return

        try:
            standard_packet = {}

            # If it's a Join message, then associate DevEUI with DevAddr

            if msg.topic[-5:] == "/join":
                device_info = {'dev_eui': mqtt_messsage.get('devEUI', None)}
                client.devices_map[mqtt_messsage['devAddr']] = device_info

                # Save this message an topic into MQ
                client.packet_writter_message['messages'].append(
                    {
                        'topic': msg.topic,
                        'message': msg.payload.decode("utf-8"),
                        'data_collector_id': client.data_collector_id
                    }
                )
                save(client.packet_writter_message, client.data_collector_id)

                # Reset packet_writter_message
                client.packet_writter_message = self.init_packet_writter_message()

                return

            # From topic gateway/gw_id/tx or gateway/gw_id/tx
            search = re.search('gateway/(.*)?/*', msg.topic)

            if search is not None and search.group(0)[-2:] in ["rx", "tx", "up"]:

                if 'phyPayload' in mqtt_messsage:
                    # PHYPayload shouldn't exceed 255 bytes by definition. In DB we support 300 bytes

                    if len(mqtt_messsage['phyPayload']) > 300:
                        return
                    # Parse the base64 PHYPayload
                    standard_packet = phy_parser.setPHYPayload(mqtt_messsage.get('phyPayload'))
                    # Save the PHYPayload
                    standard_packet['data'] = mqtt_messsage.get('phyPayload')

                if is_protobuf_message:
                    if 'rxInfo' in mqtt_messsage:
                        x_info = mqtt_messsage.get('rxInfo')
                        standard_packet['gateway'] = base64.b64decode(x_info.get('gatewayID')).hex()
                        standard_packet['chan'] = x_info.get('channel')
                        standard_packet['rfch'] = x_info.get('rfChain')
                        standard_packet['stat'] = get_crc_status_integer(x_info.get('crcStatus')) # When protobuf is deserialized, this is a string, but we need to send an integer
                        standard_packet['rssi'] = x_info.get('rssi')
                        standard_packet['lsnr'] = x_info.get('loRaSNR')
                        standard_packet['size'] = x_info.get('size')

                    if 'txInfo' in mqtt_messsage:
                        x_info = mqtt_messsage.get('txInfo')
                        standard_packet['freq'] = x_info.get('frequency') / 1000000 if 'frequency' in x_info else None
                        lora_modulation_info= x_info.get('loRaModulationInfo')
                        standard_packet['datr'] = json.dumps({"spread_factor": lora_modulation_info.get('spreadingFactor'),
                                                        "bandwidth": lora_modulation_info.get('bandwidth')})
                        standard_packet['codr'] = lora_modulation_info.get('codeRate')
                else:
                    if 'rxInfo' in mqtt_messsage:
                        x_info = mqtt_messsage.get('rxInfo')
                        standard_packet['chan'] = x_info.get('channel')
                        standard_packet['rfch'] = x_info.get('rfChain')
                        standard_packet['stat'] = x_info.get('crcStatus')
                        standard_packet['codr'] = x_info.get('codeRate')
                        standard_packet['rssi'] = x_info.get('rssi')
                        standard_packet['lsnr'] = x_info.get('loRaSNR')
                        standard_packet['size'] = x_info.get('size')

                    if 'txInfo' in mqtt_messsage:
                        x_info= mqtt_messsage.get('txInfo')

                    standard_packet['tmst'] = x_info.get('timestamp')
                    standard_packet['freq'] = x_info.get('frequency') / 1000000 if 'frequency' in x_info else None
                    standard_packet['gateway'] = x_info.get('mac')

                    data_rate= x_info.get('dataRate')
                    standard_packet['modu'] = data_rate.get('modulation')
                    standard_packet['datr'] = json.dumps({"spread_factor": data_rate.get('spreadFactor'),
                                                        "bandwidth": data_rate.get('bandwidth')})

                # Add missing fields, independant from type of packet
                standard_packet['topic'] = msg.topic
                standard_packet['date'] = datetime.now().__str__()
                standard_packet['data_collector_id'] = client.data_collector_id
                standard_packet['organization_id'] = client.organization_id

                # Save prev_packet in case is not empty

                if client.prev_packet is not None:
                    client.packet_writter_message['packet'] = client.prev_packet
                    save(client.packet_writter_message, client.data_collector_id)

                    # Reset variables
                    client.prev_packet = None
                    client.packet_writter_message = self.init_packet_writter_message()

                # Set the dev_eui and other information if available. Otherwise, save packet

                if 'dev_addr' in standard_packet:

                    if standard_packet['dev_addr'] in client.devices_map:
                        standard_packet['dev_eui'] = client.devices_map[standard_packet['dev_addr']]['dev_eui']

                        if len(client.devices_map[standard_packet['dev_addr']]) > 1:
                            standard_packet['app_name'] = client.devices_map[standard_packet['dev_addr']]['app_name']
                            standard_packet['dev_name'] = client.devices_map[standard_packet['dev_addr']]['dev_name']

                    else:
                        # Save this packet for now
                        client.prev_packet = standard_packet
                        # Save the message and topic as well
                        client.packet_writter_message['messages'].append(
                            {
                                'topic': msg.topic,
                                'message': json.dumps(mqtt_messsage) if is_protobuf_message else msg.payload.decode("utf-8"),
                                'data_collector_id': client.data_collector_id
                            }
                        )
                self.last_seen = datetime.now()

            # From topic application/*/device/*/rx or application/*/node/*/rx
            elif re.search('application/.*?/device/(.*)/rx', msg.topic) is not None or re.search(
                    'application/.*?/node/(.*)/rx', msg.topic) is not None:

                search = re.search('application/.*?/device/(.*)/rx', msg.topic)

                if search is None:
                    search = re.search('application/.*?/node/(.*)/rx', msg.topic)

                if client.prev_packet is not None:
                    standard_packet = client.prev_packet
                    client.prev_packet = None

                    if standard_packet['f_count'] == mqtt_messsage.get('fCnt', None):
                        # Set location and gw name if given

                        if 'rxInfo' in mqtt_messsage:
                            location = mqtt_messsage.get('rxInfo', None)[0].get('location', None)

                            if location:
                                standard_packet['latitude'] = location.get('latitude', None)
                                standard_packet['longitude'] = location.get('longitude', None)
                                standard_packet['altitude'] = location.get('altitude', None)

                            gw_name= mqtt_messsage.get('rxInfo')[0].get('name')
                            standard_packet['gw_name'] = gw_name if gw_name else None

                        # Make sure we've matched the same device

                        if 'dev_eui' in standard_packet and standard_packet['dev_eui'] is not None and standard_packet[
                            'dev_eui'] != search.group(1):
                            self.log.error("There's an error with Chirsptack collector logic")

                        # Get dev_eui, app_name and dev_name from message
                        device_info = {'app_name': mqtt_messsage.get('applicationName', None),
                                       'dev_name': mqtt_messsage.get('deviceName', None),
                                       'dev_eui': mqtt_messsage.get('devEUI', None)}
                        client.devices_map[standard_packet['dev_addr']] = device_info

                        # Set previous values to current message
                        standard_packet['dev_eui'] = client.devices_map[standard_packet['dev_addr']]['dev_eui']

                        if len(client.devices_map[standard_packet['dev_addr']]) > 1:
                            standard_packet['app_name'] = client.devices_map[standard_packet['dev_addr']]['app_name']
                            standard_packet['dev_name'] = client.devices_map[standard_packet['dev_addr']]['dev_name']

                self.last_seen = datetime.now()

            else:
                # First, check if we had a prev_packet. If so, first save it

                if client.prev_packet is not None and len(standard_packet) == 0:
                    client.packet_writter_message['packet'] = client.prev_packet
                    save(client.packet_writter_message, client.data_collector_id)

                    # Reset vars
                    client.packet_writter_message = self.init_packet_writter_message()
                    client.prev_packet = None

                # Save SKIPPED MQ message and topic
                client.packet_writter_message['messages'].append(
                    {
                        'topic': msg.topic,
                        'message': msg.payload.decode("utf-8"),
                        'data_collector_id': client.data_collector_id
                    }
                )
                save(client.packet_writter_message, client.data_collector_id)

                # Reset packet_writter_message
                client.packet_writter_message = self.init_packet_writter_message()

                return

            # Save packet

            if client.prev_packet is None and len(standard_packet) > 0:
                # Save packet JSON
                client.packet_writter_message['packet'] = standard_packet

                # Save MQ message and topic
                client.packet_writter_message['messages'].append(
                    {
                        'topic': msg.topic,
                        'message': json.dumps(mqtt_messsage) if is_protobuf_message else msg.payload.decode("utf-8"),
                        'data_collector_id': client.data_collector_id
                    }
                )

                save(client.packet_writter_message, client.data_collector_id)

                # Reset packet_writter_message obj
                client.packet_writter_message = self.init_packet_writter_message()

        except Exception as e:
            self.log.error("Error creating Packet in Chirpstack collector:", e, "Topic: ", msg.topic, "Message: ",
                      json.dumps(mqtt_messsage) if is_protobuf_message else msg.payload.decode("utf-8"))
            traceback.print_exc(file=sys.stdout)
            save_parsing_error(client.data_collector_id, json.dumps(mqtt_messsage) if is_protobuf_message else msg.payload.decode("utf-8"))

    def on_connect(self, client, userdata, flags, rc):
        # If this connection is a test, activate the flag and emit the event

        if self.being_tested:
            notify_test_event(client.data_collector_id, 'SUCCESS', 'Connection successful')
            self.stop_testing = True

            return
        else:
            client.subscribe(client.topics)
            self.connected = "CONNECTED"
        self.log.info("Connected to: {} with result code: {}".format(client.host, rc))

    def on_disconnect(self, client, userdata, rc):
        if self.being_tested:
            return

        if rc != 0:
            self.connected = "DISCONNECTED"
            self.log.info("Unexpected disconnection.")

def get_crc_status_integer(status_string):
    # This mapping is in https://github.com/brocaar/chirpstack-api/blob/master/protobuf/gw/gw.proto

    if status_string=='CRC_OK':
        return 1
    elif status_string=='BAD_CRC':
        return -1
    elif status_string=='NO_CRC':
        return 0

if __name__ == '__main__':
    from auditing.db.Models import DataCollector, DataCollectorType, Organization, commit, rollback

    print("\n*****************************************************")
    print("LoRaWAN Security Framework - %s" % (sys.argv[0]))
    print("Copyright (c) 2019 IOActive Inc.  All rights reserved.")
    print("*****************************************************\n")

    parser = argparse.ArgumentParser(
        description='This script connects to the mqqt broker and saves messages into the DB. You must specify a unique collectorID and you can specify the topics you want to suscribe to.')

    parser.add_argument('--ip',
                        help='MQTT broker ip, eg. --ip 192.168.3.101.')
    parser.add_argument('--port',
                        help='MQTT broker port, eg. --port 623.',
                        type=int)
    parser.add_argument('--collector-id',
                        help='The ID of the dataCollector. This ID will be associated to the packets saved into DB. eg. --id 1')
    parser.add_argument('--organization-id',
                        help='The ID of the dataCollector. This ID will be associated to the packets saved into DB. eg. --id 1',
                        default=None)
    parser.add_argument('--topics',
                        nargs='+',
                        help='List the topic(s) you want to suscribe separated by spaces. If nothing given, default will be "#.',
                        default="#")

    options = parser.parse_args()

    if options.topics != None:
        topics = list()

        for topic in options.topics:
            topics.append((topic, 0))

    # Get the organization

    if options.organization_id:
        organization_obj = Organization.find_one(options.organization_id)

        if organization_obj is None:
            print("Organization doesn't exist. Please provide a valid ID")
            exit(0)

    else:
        organization_quant = Organization.count()

        if organization_quant > 1:
            print("There are more than one organizations in the DB. Provide the Organization DB explicitly.")

        elif organization_quant == 1:
            organization_obj = Organization.find_one()

        else:
            organization_obj = Organization(name="Auto-generated Organization")
            organization_obj.save()

    # Get the data collector
    collector_obj = None

    if options.collector_id:
        collector_obj = DataCollector.find_one(options.collector_id)

        if collector_obj is None:
            print("DataCollector doesn't exist. Please provide a valid ID")
            exit(0)

    else:

        if options.ip and options.port:
            collector_type_obj = DataCollectorType.find_one_by_type("loraserverio_collector")

            if collector_type_obj is None:
                collector_type_obj = DataCollectorType(
                    type="loraserverio_collector",
                    name="loraserverio_collector")
                collector_type_obj.save()

            collector_obj = DataCollector.find_one_by_ip_port_and_dctype_id(collector_type_obj.id, options.ip,
                                                                            str(options.port))

            if collector_obj is None:
                collector_obj = DataCollector(
                    data_collector_type_id=collector_type_obj.id,
                    name="Test collector",
                    organization_id=organization_obj.id,
                    ip=options.ip,
                    port=str(options.port),
                    last_seen=datetime.now(),
                    connected=False)
                collector_obj.save()

        else:
            print('Datacollector IP and port must be provided if not provided a collector ID.')
            exit(0)
    collector = LoraServerIOCollector(
        data_collector_id=collector_obj.id,
        organization_id=collector_obj.organization_id,
        host=collector_obj.ip,
        port=int(collector_obj.port),
        ssl=collector_obj.ssl,
        user=None,
        password=None,
        topics=topics,
        last_seen=collector_obj.last_seen,
        connected=collector_obj.connected,
        verified=collector_obj.verified,
        ca_cert=collect_obj.ca_cert,
        client_cert=collect_obj.client_cert,
        client_key=collect_obj.client_key)

    collector.connect()

    while (True):
        time.sleep(5)
        try:
            commit()
        except Exception as exc:
            self.log.error('Error at commit:' + str(exc))
            self.log.info('Rolling back the session')
            rollback()
