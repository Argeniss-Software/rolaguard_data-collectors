from datetime import datetime, timedelta
from threading import Timer
from time import sleep

from auditing import iot_logging
from auditing.datacollectors.utils.PacketPersistence import notify_test_event, notify_status_event


class BaseCollector:

    def __init__(self, data_collector_id, organization_id, verified,
                 minimum_packets=5, verification_threshold=0.8, verification_timeout=600.0):
        self.data_collector_id = data_collector_id
        self.organization_id = organization_id
        self.connected = "DISCONNECTED"
        self.disabled = False
        self.log = iot_logging.getLogger(f'{self.__class__.__name__} ({data_collector_id})')

        self.__verified = verified
        self.verified_changed = False
        self.verified_packets = 0
        self.total_packets = 0
        self.minimum_packets = minimum_packets
        self.verification_threshold = verification_threshold
        self.verification_timeout = verification_timeout
        self.verification_timer = Timer(verification_timeout, self.verify_timeout)

    # region verified property
    def setverified(self, value):
        self.__verified = value
        if self.verification_timer:
            self.verification_timer.cancel()
        self.verified_changed = True

    def getverified(self):
        return self.__verified

    verified = property(getverified, setverified)

    # endregion

    def verify_timeout(self):
        # if the collector was verified, the timer should have been cancelled.
        # Even if it was not cancelled, it will do nothing
        if not self.verified:
            self.disconnect()
            self.disabled = True
            notify_status_event(self.data_collector_id,
                                {'data_collector_id': self.data_collector_id,
                                 'message': f'Collector not verified after {self.verification_timeout} seconds',
                                 'type': 'FAILED_VERIFY'})

    def init_packet_writter_message(self):
        return {'packet': None, 'messages': list()}

    def connect(self):
        if not (self.being_tested or self.verified):
            self.verification_timer.start()

    def disconnect(self):
        self.verification_timer.cancel()

    def test(self):
        self.log.info(f'Testing connection')
        self.being_tested = True
        self.stop_testing = False

        # Try to connect the collector as in regular scenario
        self.connect()
        timeout = datetime.now() + timedelta(seconds=30)
        while not self.stop_testing and datetime.now() < timeout:
            sleep(1)
        self.disconnect()

        if not self.stop_testing:  # Means that the collector didn't have any update within the timeout
            message = 'Timeout error of {0} seconds exceeded.'.format(timeout)
            notify_test_event(self.data_collector_id, 'ERROR', message)

        self.disconnect()


    def verify_message(self, msg):
        """
        Verifies actual message and updates internal counters
        if ratio verified_packets/total_packets > verification_threshold, sets verified=True
        """
        self.total_packets += 1
        if not self.verify_payload(msg):
            self.log.error("PHY payload could not be verified")
            return False

        if not self.verify_topics(msg):
            self.log.error("Topic is not usable")
            return False

        self.verified_packets += 1
        if self.total_packets >= self.minimum_packets \
                and (self.verified_packets / self.total_packets) > self.verification_threshold:
            self.verified = True
            return True
        return False

    def verify_payload(self, msg):
        return True

    def verify_topics(self, msg):
        return True
