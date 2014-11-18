import re
from tempoiq.protocol import Device, Sensor


class MigrationScheme(object):
    name = "MigrationScheme"
    db_key = ""
    db_secret = ""
    db_baseurl = "https://api.tempo-db.com/v1/"

    iq_key = ""
    iq_secret = ""
    iq_baseurl = "https://sandbox-matt.backend.tempoiq.com"

    def identity_series_filter(self):
        """Filter that will return one TempoDB series for every device that
        should be created. Filter is a (keys, tags, attrs) tuple."""
        raise NotImplementedError("identity_series_filter not implemented")

    def identity_series_client_filter(self, series):
        """For cases when we can't get an identity series via server-side
        filtering. Take a series object (presumably from listing all series
        in a DB), and return True if we should make a Device from it."""
        return True

    def series_client_filter(self, series, identity_series):
        """Return False if a series shouldn't actually be included with the
        given identity series"""
        return True

    def series_to_filter(self, series):
        """Given a TempoDB identity series object, return a filter for all
        series that should be included in the same device."""
        raise NotImplementedError("series_to_filter not implemented")

    def all_series_to_device(self, series_list):
        """Given a list of all TempoDB series belonging to the same device, return
        the corresponding IQ device object"""
        raise NotImplementedError("series_to_device not implemented")

    def series_to_device_key(self, series):
        self.series_key_to_device_key(series.key)

    def split_series_key(self, key):
        """Take a series key and return a tuple of (devicekey, sensorkey)"""
        raise NotImplementedError("split_series_key not implemented")

    def series_key_to_sensor_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return sensor

    def series_key_to_device_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return dev

