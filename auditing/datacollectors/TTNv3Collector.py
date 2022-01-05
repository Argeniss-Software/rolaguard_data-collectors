import os
import threading
import json
import pycurl
import validators
from datetime import datetime
from time import sleep
import dateutil.parser
import auditing.datacollectors.utils.PhyParser as phy_parser
from auditing.datacollectors.BaseCollector import BaseCollector
from auditing.datacollectors.utils.PacketPersistence import save, save_parsing_error, save_login_error, \
    notify_test_event
from auditing.db.TTNRegion import TTNRegion

STREAM_TIMEOUT = 1800  # 30 mins

stream_eu1_url = os.environ['self.STREAM_EU1_URL'] if 'self.STREAM_EU1_URL' in os.environ else 'https://eu1.cloud.thethings.network/api/v3/events'
stream_nam1_url = os.environ['self.STREAM_NAM1_URL'] if 'self.STREAM_NAM1_URL' in os.environ else 'https://nam1.cloud.thethings.network/api/v3/events'
stream_au1_url = os.environ['self.STREAM_AU1_URL'] if 'self.STREAM_AU1_URL' in os.environ else 'https://au1.cloud.thethings.network/api/v3/events'

class TTNv3Collector(BaseCollector):

    """
    This collector establishes a connection to a thethingsnetwork.com account and 
    retrieves data from the https://<region>.cloud.thethings.network/api/v3/events endpoint using Curl.

    The steps to retrieve gateway payloads:
    1- Connect to the stream event using the values gateway_name and api_key provided by the user.
    2- Handle messages with the message() function.

    There are different kinds of messages, the most important are:
    * gateway downlink and gateway uplink: this is, uplink and downlink data messages
    (PHYpayload) as well as radio metadata. Uplinks are received under the name "gs.up.receive" and downlinks under the name "gs.down.send".
    * join requests, which are received under the name "gs.up.receive", and join accept, which come under the name "gs.down.send".
    * gateway status: it provides the location of the gateway. Come under the name "gs.status.receive".

    About the functioning of this collector:
    1- It's instantiated in the Orchestrator, and it's started by executing connect()
    method.
    2- In connect() a thread is launched to start the stream, where new messages are checked every second and processed. The connection is restarted every 30 minutes to avoid the disconnection from the server.
    """

    def __init__(self, data_collector_id, organization_id, api_key, gateway_name, region_id, verified, host, port):
        super().__init__(data_collector_id=data_collector_id,
                         organization_id=organization_id, verified=verified)
        self.api_key = api_key
        self.gateway_name = gateway_name
        if region_id is None:
            self.region = None
        else:
            self.region = TTNRegion.find_region_by_id(int(region_id))
        self.last_seen = None
        self.manually_disconnected = None
        self.packet_writter_message = self.init_packet_writter_message()
        self.stream_thread = None
        self.location = dict()  # Dict containing location
        self.being_tested = False
        self.host = host
        self.port = port

    def connect(self):
        if self.stream_thread is None:
            super(TTNv3Collector, self).connect()

            self.stream_thread = threading.Thread(
                target=self.run_stream, args=())
            self.stream_thread.daemon = True
            self.stream_thread.start()
        else:
            self.log.error(
                f'Error starting stream thread for gw {self.gateway_name}, another thread is alive')

    def on_receive(self, data):
        decoded_data = data[:-2].decode() # Each message ends with '\n\n'
        # If more than one message was read, we have to split them and
        # process each one independently
        decoded_data = decoded_data.split('\n\n')
        for msg in decoded_data:
            self.message(msg)

    def run_stream(self):
        init_connection = True

        headers = [
            'Accept: text/event-stream',
            'Authorization: Bearer ' + self.api_key
        ]
        post_data = {'identifiers': [
            {'gateway_ids': {'gateway_id': self.gateway_name}}
        ]}
        
        if self.region is None:
            if validators.url(self.host):
                stream_url=self.host
            elif validators.domain(self.host):
                stream_url=self.host+':'+str(self.port)
        else:
            if self.region == 'eu1':
                stream_url = stream_eu1_url
            elif self.region == 'nam1':
                stream_url = stream_nam1_url
            elif self.region == 'au1':
                stream_url = stream_au1_url

        while True:
            if init_connection:
                curl = pycurl.Curl()
                curl.setopt(pycurl.HTTPHEADER, headers)
                curl.setopt(pycurl.URL, stream_url)
                curl.setopt(pycurl.WRITEFUNCTION, self.on_receive)
                curl.setopt(pycurl.POSTFIELDS, json.dumps(post_data))
                curl.setopt(pycurl.TIMEOUT, STREAM_TIMEOUT)

                multi_curl = pycurl.CurlMulti()
                multi_curl.add_handle(curl)

                init_connection = False

            multi_curl.perform()
            status_code = curl.getinfo(pycurl.RESPONSE_CODE)

            if status_code == 0:
                sleep(1)
            elif status_code == 200:
                if self.being_tested:
                    notify_test_event(self.data_collector_id,
                                      'SUCCESS', 'Connection successful')
                    self.stop_testing = True

                self.connected = 'CONNECTED'
                self.manually_disconnected = None

                while True:
                    if self.manually_disconnected:
                        curl.close()
                        multi_curl.close()
                        del multi_curl, curl
                        return

                    multi_curl.perform()
                    error = curl.errstr()

                    if error == '':
                        pass
                    elif 'Operation timed out' in error:
                        # Restart the connection every STREAM_TIMEOUT secs
                        curl.close()
                        multi_curl.close()
                        init_connection = True
                        break
                    else:
                        self.connected = 'DISCONNECTED'
                        curl.close()
                        multi_curl.close()
                        del multi_curl, curl
                        self.log.error(
                            f'Error reading data in TTNCollector ID {self.data_collector_id}: {error}')
                        return

                    sleep(1)
            else:
                self.connected = 'DISCONNECTED'
                curl.close()
                multi_curl.close()
                del multi_curl, curl
                if self.being_tested:
                    notify_test_event(self.data_collector_id,
                                      'ERROR', 'Connection failed')
                    self.stop_testing = True
                else:
                    save_login_error(self.data_collector_id)
                return

    def disconnect(self):
        self.manually_disconnected = True
        self.connected = 'DISCONNECTED'

        if self.being_tested:
            self.log.info(
                f'Stopping test connection to DataCollector ID {self.data_collector_id}')
        else:
            self.log.info(
                f'Manually disconnected to gw: {self.gateway_name}')

        sleep(2)
        if self.stream_thread is None:
            self.log.info(
                f'Stream thread from gw {self.gateway_name} is already closed')
        elif self.stream_thread.is_alive():
            self.log.error(
                f'Error stopping stream thread for gw {self.gateway_name}: thread is alive')
        else:
            self.stream_thread = None

        super(TTNv3Collector, self).disconnect()

    def verify_payload(self, msg):
        # If we managed to login into TTN, then we are sure we're receiving TTN messages.
        # Then, I comment the code below
        return True

        # if not self.has_to_parse:
        #     return True  # NOT SURE if this should be True or False

        # phyPayload = msg.get('payload', None)
        # if not phyPayload:
        #     self.log.error("Payload not present in message")
        #     return False
        # try:
        #     phy_parser.setPHYPayload(phyPayload)
        #     return True
        # except Exception as e:
        #     self.log.error(f'Error parsing physical payload: {e}')
        #     return False

    def message(self, raw_message):
        if self.being_tested:
            return

        try:
            message = json.loads(raw_message)['result']
            message_data = message.get('data')
            name = message.get('name')

            if name == 'events.stream.start':
                return
            elif name == 'gs.up.receive' or name == 'gs.down.send':
                self.has_to_parse = True
            else:
                self.has_to_parse = False

            if not self.verified:
                # TTN collectors only verify the physical payload, which is only parsed if has_to_parse is True
                if not self.verify_message(message):
                    self.log.debug(
                        f'Collector is not yet verified ({self.verified_packets} verified), skipping message\n')
                    return

            # Message processing
            if name == 'gs.status.receive' and message_data.get('antenna_locations', None):
                # Check if the location is given in this message. If so, save it and add it in subsequent messages
                try:
                    self.location['longitude'] = message_data.get(
                        'antenna_locations')[0].get('longitude')
                    self.location['latitude'] = message_data.get(
                        'antenna_locations')[0].get('latitude')
                    self.location['altitude'] = message_data.get(
                        'antenna_locations')[0].get('altitude')
                except Exception as e:
                    self.log.error(
                        f'Error when fetching location in TTNCollector: {str(e)}  Message: {raw_message}')

            # Save the message that originates the packet
            self.packet_writter_message['messages'].append(
                {
                    'topic': None,
                    'message': raw_message[0:4096],
                    'data_collector_id': self.data_collector_id
                }
            )

            self.last_seen = datetime.now()

            if self.has_to_parse:
                packet = phy_parser.setPHYPayload(
                    message_data.get('raw_payload'))
                packet['chan'] = None
                packet['stat'] = None

                rx_metadata = message_data.get('rx_metadata', None)
                if rx_metadata:
                    packet['lsnr'] = rx_metadata[0].get('snr', None)
                    packet['rssi'] = rx_metadata[0].get('rssi', None)
                else:
                    packet['lsnr'] = None
                    packet['rssi'] = None

                tmst = message.get('time', None)
                if tmst:
                    packet['tmst'] = datetime.timestamp(
                        dateutil.parser.parse(tmst))
                else:
                    packet['tmst'] = None

                if name == 'gs.up.receive':
                    settings = message_data.get('settings', None)
                    if settings:
                        packet['freq'] = int(settings.get(
                            'frequency', None))/1000000
                        packet['codr'] = settings.get('coding_rate', None)
                    else:
                        packet['freq'] = None
                        packet['codr'] = None
                else:  # name == 'gs.down.send':
                    request = message_data.get('request', None)
                    if request:
                        # rx1_frequency is stored as freq, rx2_frequency isn't stored
                        packet['freq'] = int(request.get(
                            'rx1_frequency', None))/1000000
                    else:
                        packet['freq'] = None
                    packet['codr'] = None

                packet['rfch'] = None
                packet['modu'] = None
                packet['datr'] = None
                packet['size'] = None
                packet['data'] = message_data.get('raw_payload')

                if len(self.location) > 0:
                    packet['latitude'] = self.location['latitude']
                    packet['longitude'] = self.location['longitude']
                    packet['altitude'] = self.location['altitude']

                    # Reset location
                    self.location = {}

                packet['app_name'] = None
                packet['dev_name'] = None

                identifiers = message.get('identifiers', None)
                if identifiers:
                    packet['gateway'] = identifiers[0]['gateway_ids']['eui']
                else:
                    packet['gateway'] = None

                packet['gw_name'] = self.gateway_name

                packet['seqn'] = None
                packet['opts'] = None
                packet['port'] = None

                # If dev_eui couldn't be fetched from raw_payload
                if packet.get('dev_eui', None) is None:
                    packet['dev_eui'] = None

                packet['date'] = datetime.now().__str__()
                packet['data_collector_id'] = self.data_collector_id
                packet['organization_id'] = self.organization_id

                self.packet_writter_message['packet'] = packet

            # Save the packet
            save(self.packet_writter_message, self.data_collector_id)

            # Reset this variable
            self.packet_writter_message = self.init_packet_writter_message()

        except Exception as e:
            self.log.error(
                f'Error creating Packet in TTNCollector ID {self.data_collector_id}: {str(e)} Message: {raw_message}')
            save_parsing_error(self.data_collector_id, raw_message)
