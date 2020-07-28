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

    """
    This collector establishes a connection to a thethingsnetwork.com account and 
    retrieves data from the https://console.thethingsnetwork.org/gateways/eui-THEGATEWAYID/traffic
    endpoint using websockets.

    The steps to retrieve gateway payloads:
    1- Get the access token
    2- Using the access token and the Gateway ID (see below for its format), suscribe
    to the web socket.
    3- Handle messages with the on_message() function.

    There are 5 kinds of messages:
    * gateway downlink and gateway uplink: this is, uplink and downlink data messages
    (PHYpayload) as well as radio metadata.
    * join request and join accept
    * gateway status: it provides the location of the gateway.

    About the functioning of this collector:
    1- It's instantiated in the Orchestrator, and it's started by executing connect()
    method
    2- In connect(), 2 threads are launched: 
        a- one for the WS socket connection, where messages are processed. In case we
        receive a disconnection message from the server, the refresh token thread is 
        stopped and the connect() method is executed again.
        b- and the other thread is for refreshing the access token every N minutes. In
        the case where is not possible to send the new access token to the web server,
        and after 3 consecutive failed attemps, this thread stops the WS thread and 
        executes the connect() method.

    Some considerations about this endpoint / websocket:
    * It provides messages with the same amount of information as the packet_forwarder,
    which means that NO application data is handled (such as direct associations)
    between message/devEui, app_name, gw_name, etc.
    * Sometimes it happens that the access token is retrieved but the web 
    service refuses to accept it. In this situation, we manually restart the
    WS and open a new connection.

    About the Gateway ID format, it can be:
    * Legacy packet-forwarder format: a string matching the pattern 'eui-aabbccddeeff0011'. 
    This is, 'eui-' followed by 8 bytes in hex format (16 characters), or,
    * TTN format: a lowercase alphanumeric string separated by hyphens.  

    """

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
        self.ws_thread= None
        self.refresh_token_thread= None

    def connect(self):
        super(TTNCollector, self).connect()
        self.session = self.login(self.user, self.password)
        if self.session:
            self.connected = "CONNECTED"
            self.manually_disconnected = None
            data_access = self.fetch_access_token(self.session)
            access_token = data_access.get('access_token')
            expires = data_access.get('expires')

            self.ws = websocket.WebSocketApp(ws_url,
                                             on_message=lambda ws, msg: self.on_message(ws, msg),
                                             on_error=lambda ws, msg: self.on_error(ws, msg),
                                             on_close=lambda ws: self.on_close(ws))
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

            self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={'ping_interval': 20})
            self.ws_thread.daemon = True
            self.ws_thread.start()

            self.refresh_token_thread = threading.Thread(target=self.schedule_refresh_token, args=(self.ws, self.session, expires))
            self.refresh_token_thread.daemon = True
            self.refresh_token_thread.start()
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

        # Retry after disconnection. End thread refreshing token before
        if '[200,"disconnected"]' in raw_message:
            self.log.info(f"DataCollector {self.data_collector_id}: Disconnected by server. Reconnecting.")
            ws.close()
            ws.is_closed= True
            self.log.debug(f"DataCollector {self.data_collector_id}: Joining refresh token thread.")
            self.refresh_token_thread.join()
            self.log.debug(f"DataCollector {self.data_collector_id}: Refresh token thread joined.")
            self.connect()

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
                    self.log.error(f"Error when fetching location in TTNCollector: {str(e)}  Message: {raw_message}" )
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

            self.log.debug(f'Message received from TTN saved in DB: {ws.packet_writter_message}.')

            # Reset this variable
            ws.packet_writter_message = self.init_packet_writter_message()

        except Exception as e:
            self.log.error(f"Error creating Packet in TTNCollector ID {ws.data_collector_id}: {str(e)} Message: {raw_message}")
            save_parsing_error(ws.data_collector_id, raw_message)

    def on_error(self, ws, error):
        # If this connection is a test, send the event
        if self.being_tested:
            notify_test_event(self.data_collector_id, 'ERROR', str(error))
            self.log.error(f"Error testing DataCollector ID {self.data_collector_id}: {str(error)}")
            self.stop_testing = True
            return
        else:
            self.log.error(f"Error ws: {str(error)}")

    def on_close(self, ws):  # similar to on_disconnect
        ws.close()
        ws.is_closed = True
        self.log.info(f"Disconnected to gw: {ws.gateway_id}")

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
        self.log.info(f"Connected to GW: {ws.gateway}" )

    def login(self, user, password):
        ses = requests.Session()
        ses.headers['Content-type'] = 'application/json'
        res = ses.post(account_login_url, data=json.dumps({"username": user, "password": password}))
        ses.get(login_url)

        return ses if res.status_code == 200 else None

    def fetch_access_token(self, ses):
        self.log.info(f'ses cookies: {str(ses.cookies)}')
        res = ses.get(access_token_url, timeout=30)
        self.log.info(f'res: {str(res)}')
        return res.json()

    def schedule_refresh_token(self, ws, session, first_expires):
        expires = first_expires
        connection_attempts= 0
        expire_dt= None
        
        while (not ws.is_closed):
            
            if expire_dt is not None and expire_dt > datetime.now(): 
                sleep(30)
                continue

            if expires:
                expire_dt = datetime.fromtimestamp((expires / 1000)-900) # Converted from ms to seconds and substracted 15 min
                self.log.info(f"expires: {str(expires)}")
                self.log.debug(f"DataCollector {self.data_collector_id}: Refresh token in {(expire_dt - datetime.now()).seconds} seconds")
                self.log.debug(f"WS is closed: {str(ws.is_closed)}")

                if first_expires:
                    first_expires=None
                    continue
                
            try:
                data_access = self.fetch_access_token(session)
                access_token = data_access.get('access_token')
                expires = data_access.get('expires')
                ws.access_token = access_token
                self.log.info(f"access token: {access_token}")
                ws.send('["token:' + access_token + '"]')
                connection_attempts= 0
            except Exception as exc:
                self.log.error(f'error fetching access token: {str(exc)}')
                expires= None
                expire_dt= None
                connection_attempts+=1
                if connection_attempts>= 3:
                    self.log.info(f"DataCollector {self.data_collector_id}: Stopping websocket")
                    self.ws.close()
                    self.ws_thread.join()
                    self.ws= None
                    self.log.info(f"DataCollector {self.data_collector_id}: Reconnecting websocket")
                    self.connect()
        
        self.log.info(f"DataCollector {self.data_collector_id}: Stop token refresh")
        
