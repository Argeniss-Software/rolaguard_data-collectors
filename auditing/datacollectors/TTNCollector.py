import os
import websocket
import threading
import json
import requests
from datetime import datetime, timedelta
import dateutil.parser
from time import sleep
import auditing.datacollectors.utils.PhyParser as phy_parser
from auditing.datacollectors.BaseCollector import BaseCollector
from auditing.datacollectors.utils.PacketPersistence import save, save_parsing_error, save_login_error, \
    notify_test_event

account_login_url = os.environ[
    'ACCOUNT_self.logIN_URL'] if 'ACCOUNT_self.logIN_URL' in os.environ else 'https://account.thethingsnetwork.org/api/v2/users/login'  # 'https://account.thethingsnetwork.org/api/v2/users/login'
login_url = os.environ['self.logIN_URL'] if 'self.logIN_URL' in os.environ else 'https://console.thethingsnetwork.org/login'
access_token_url = os.environ[
    'ACCESS_TOKEN_URL'] if 'ACCESS_TOKEN_URL' in os.environ else 'https://console.thethingsnetwork.org/refresh'
ws_url = os.environ[
    'WS_URL'] if 'WS_URL' in os.environ else 'wss://console.thethingsnetwork.org/api/events/644/lta0xryg/websocket?version=v2.6.11'


class TTNCollector(BaseCollector):
    def __init__(self, data_collector_id, organization_id, user, password, gateway_id, verified):
        super().__init__(data_collector_id=data_collector_id, organization_id=organization_id, verified=verified)
        self.user = user
        self.password = password
        self.gateway_id = gateway_id
        self.ws = None
        self.session = None
        self.last_seen = None
        self.manually_disconnected = None
        # The data sent to the MQTT queue, to be written by the packet writer. It must have at least one MQ message
        self.packet_writter_message = self.init_packet_writter_message()
        # Dict containing location 
        self.location = dict()
        self.being_tested = False

    def connect(self):
        super(TTNCollector, self).connect()
        self.session = self.login(self.user, self.password)
        if self.session:
            self.connected = "DISCONNECTED"
            self.manually_disconnected = None
            data_access = self.fetch_access_token(self.session)
            access_token = data_access.get('access_token')
            expires = data_access.get('expires')

            self.ws = websocket.WebSocketApp(ws_url,
                                             on_message=lambda ws, msg: self.on_message(ws, msg),
                                             on_error=lambda ws, msg: self.on_error(ws, msg),
                                             on_close=lambda ws, msg: self.on_close(ws))
            self.log.debug(f'WebSocket app initialized')
            self.ws.access_token = access_token
            self.ws.gateway = self.gateway_id
            self.ws.organization_id = self.organization_id
            self.ws.data_collector_id = self.data_collector_id
            self.ws.on_open = lambda ws: self.on_open(ws)
            self.ws.user_data = self
            self.ws.is_closed = False

            self.ws.packet_writter_message = self.packet_writter_message
            self.ws.location = self.location

            thread = threading.Thread(target=self.ws.run_forever)
            thread.daemon = True
            thread.start()

            thread = threading.Thread(target=self.schedule_refresh_token, args=(self.ws, self.session, expires))
            thread.daemon = True
            thread.start()
        else:
            if self.being_tested:
                notify_test_event(self.data_collector_id, 'ERROR', 'Login failed')
                self.stop_testing = True
            else:
                save_login_error(self.data_collector_id)

    def disconnect(self):
        self.manually_disconnected = True

        if self.being_tested:
            self.log.info("Stopping test connection to DataCollector ID {0}".format(self.data_collector_id))
        else:
            self.log.info("Manually disconnected to gw: {}".format(self.gateway_id))

        try:
            if self.ws:
                self.ws.close()
        except Exception as exc:
            self.log.error("Error closing socket: " + str(exc))
        super(TTNCollector, self).disconnect()

    def verify_payload(self, msg):
        # If we managed to login into TTN, then we are sure we're receiving TTN messages.
        # Then, I comment the code below
        return True
        
        # if not self.has_to_parse:
        #     self.log.debug('message does not include physical payload')
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

    def on_message(self, ws, raw_message):
        if self.being_tested:
            return

        # The contents of many messages is an 'h'. We don't want to print that.
        if len(raw_message) > 1:
            self.log.debug("Message: {}".format(raw_message))
        else:
            self.log.debug('Message len <= 1, skipping')
            return

        # Remove data format stuff
        message = raw_message.replace('\\"', '"')
        origin_message = message

        self.has_to_parse = False
        if 'gateway downlink' in message:
            self.has_to_parse = True
            message = message[20:-2]
        elif 'gateway uplink' in message:
            self.has_to_parse = True
            message = message[18:-2]
        elif 'gateway join request' in message:
            self.has_to_parse = True
            message = message[24:-2]
        elif 'gateway join accept' in message:
            self.has_to_parse = True
            message = message[23:-2]

        if not self.verified:
            # TTN collectors only verify the physical payload, which is only parsed if has_to_parse is True
            if not self.verify_message(message):
                self.log.debug("Collector is not yet verified, skipping message\n")
                return

        # message processing
        try:
            if 'gateway status' in message and 'location' in message:
                # Check if the location is given in this message. If so, save it and add it in subsequent messages
                message = message[18:-2].replace('\\"', '"')
                try:
                    status_message = json.loads(message)
                    ws.location['longitude'] = status_message.get('status').get('location').get('longitude')
                    ws.location['latitude'] = status_message.get('status').get('location').get('latitude')
                    ws.location['altitude'] = status_message.get('status').get('location').get('altitude')
                except Exception as e:
                    self.log.error("Error when fetching location in TTNCollector:" + str(e) + " Message: " + raw_message)
            message = message.replace('\\"', '"')

            # Save the message that originates the packet
            ws.packet_writter_message['messages'].append(
                {
                    'topic': None,
                    'message': origin_message[0:4096],
                    'data_collector_id': ws.data_collector_id
                }
            )

            self.last_seen = datetime.now()

            if self.has_to_parse:
                message = json.loads(message)
                packet = phy_parser.setPHYPayload(message.get('payload'))
                packet['chan'] = None
                packet['stat'] = None
                packet['lsnr'] = message.get('snr', None)
                packet['rssi'] = message.get('rssi', None)
                packet['tmst'] = datetime.timestamp(dateutil.parser.parse(message.get('timestamp', None))) * 1000
                packet['rfch'] = message.get('rfch', None)
                packet['freq'] = message.get('frequency', None)
                packet['modu'] = None
                packet['datr'] = None
                packet['codr'] = message.get('coding_rate', None)
                packet['size'] = None
                packet['data'] = message.get('payload')

                if len(ws.location) > 0:
                    packet['latitude'] = ws.location['latitude']
                    packet['longitude'] = ws.location['longitude']
                    packet['altitude'] = ws.location['altitude']

                    # Reset location
                    ws.location = {}

                packet['app_name'] = None
                packet['dev_name'] = None

                gw = ws.gateway
                packet['gateway'] = gw.replace('eui-', '') if gw else None

                packet['seqn'] = None
                packet['opts'] = None
                packet['port'] = None

                packet['date'] = datetime.now().__str__()
                packet['dev_eui'] = message.get('dev_eui')
                packet['data_collector_id'] = ws.data_collector_id
                packet['organization_id'] = ws.organization_id

                ws.packet_writter_message['packet'] = packet

            # Save the packet
            save(ws.packet_writter_message, ws.data_collector_id)

            self.log.debug('Message received from TTN saved in DB: {0}.'.format(ws.packet_writter_message))

            # Reset this variable
            ws.packet_writter_message = self.init_packet_writter_message()

        except Exception as e:
            self.log.error("Error creating Packet in TTNCollector ID " + ws.data_collector_id + ":" + str(
                e) + " Message: " + raw_message)
            save_parsing_error(ws.data_collector_id, raw_message)

    def on_error(self, ws, error):
        # If this connection is a test, send the event
        if self.being_tested:
            notify_test_event(self.data_collector_id, 'ERROR', str(error))
            self.log.error("Error testing DataCollector ID {0}: {1}".format(self.data_collector_id, str(error)))
            self.stop_testing = True
            return
        else:
            self.log.error("Error ws: {}".format(str(error)))

    def on_close(self, ws):  # similar to on_disconnect
        ws.close()
        ws.is_closed = True
        self.log.info("Disconnected to gw: {}".format(ws.gateway_id))

    def on_open(self, ws):  # similar to on_connect
        # If this connection is a test, activate the flag and emit the event
        if self.being_tested:
            notify_test_event(self.data_collector_id, 'SUCCESS', 'Connection successful')
            self.stop_testing = True
            return

        ws.send('["gateway:' + ws.gateway + '"]')
        ws.send('["token:' + ws.access_token + '"]')
        self.connected = "CONNECTED"
        ws.is_closed = False
        self.log.info("Connected to GW:" + ws.gateway)

    def login(self, user, password):
        ses = requests.Session()
        ses.headers['Content-type'] = 'application/json'
        res = ses.post(account_login_url, data=json.dumps({"username": user, "password": password}))
        ses.get(login_url)

        return ses if res.status_code == 200 else None

    def fetch_access_token(self, ses):
        self.log.info('ses' + str(ses.cookies))
        res = ses.get(access_token_url, timeout=30)
        self.log.info('res' + str(res))
        return res.json()

    def schedule_refresh_token(self, ws, session, first_expires):
        expires = first_expires
        while (not ws.is_closed):
            self.log.info("expires: " + str(expires))
            if expires:
                dt = datetime.fromtimestamp(expires / 1000)
                self.log.info("sleep: " + str((dt - datetime.now()).seconds - 60))
                sleep((dt - datetime.now()).seconds - 900)  # -15 min
                self.log.info("is closed: " + str(ws.is_closed))
            try:
                data_access = self.fetch_access_token(session)
                access_token = data_access.get('access_token')
                expires = data_access.get('expires')
                ws.access_token = access_token
                self.log.info("access token: " + access_token)
                ws.send('["token:' + access_token + '"]')
            except Exception as exc:
                self.log.error('error fetching access token: ' + str(exc))
                expires = None
