import re
import copy
from default import MigrationScheme
from tempoiq.protocol import Device, Sensor


class SingleSensor(MigrationScheme):
    db_key = ""
    db_secret = ""
    db_baseurl = "https://api.tempo-db.com/v1/"

    iq_key = ""
    iq_secret = ""
    iq_baseurl = "https://sandbox-foo.backend.tempoiq.com"

    sensor_name = "series"

    def identity_series_filter(self):
        """Filter that will return one TempoDB series for every device that
        should be created. Filter is a (keys, tags, attrs) tuple."""
        return ([], [], {})

    def identity_series_client_filter(self, series):
        """For cases when we can't get an identity series via server-side
        filtering. Take a series object (presumably from listing all series
        in a DB), and return True if we should make a Device from it."""
        return True

    def series_to_filter(self, series):
        """Given a TempoDB identity series object, return a filter for all
        series that should be included in the same device."""
        return ([series.key], [], {})

    def all_series_to_device(self, series_list):
        """Given a list of all TempoDB series belonging to the same device, return
        the corresponding IQ device object"""
        series = series_list[0]
        sensors = [Sensor(self.sensor_name)]
        attributes = copy.copy(series.attributes)
        for tag in series.tags:
            attributes[tag] = tag

        return Device(series.key, attributes=attributes, sensors=sensors)

    def split_series_key(self, key):
        """Take a series key and return a tuple of (devicekey, sensorkey)"""
        (key, self.sensor_name)

    def series_key_to_sensor_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return sensor

    def series_key_to_device_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return dev

