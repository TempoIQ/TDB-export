import re
from tempoiq.protocol import Device, Sensor


class MigrationScheme:
    db_key = ""
    db_secret = ""
    db_baseurl = "https://api.tempo-db.com/v1/"

    iq_key = ""
    iq_secret = ""
    iq_baseurl = "https://sandbox-matt.backend.tempoiq.com"

    # Filter that will return one TempoDB series for every device that
    # should be created. Filter is a (keys, tags, attrs) tuple.
    def series_filter(self):
        raise NotImplementedError("series_filter not implemented")

    # Given a TempoDB series object, return a TempoIQ device with all relevant
    # sensors attached.
    # TODO: do we need all series or can we hardcode sensors like this?
    def series_to_device(self, series):
        raise NotImplementedError("series_to_device not implemented")

    def get_id_from_key(self, series_key):
        raise NotImplementedError("get_id_from_key not implemented")

    def identity_series_client_filter(self, series):
        """For cases when we can't get an identity series via server-side
        filtering. Take a series object (presumably from listing all series
        in a DB), and return True if we should make a Device from it."""
        return True

    # Given a TempoDB series object, return a filter for all
    # series that should be included in the same device.
    def series_to_filter(self, series):
        raise NotImplementedError("series_to_filter not implemented")

    def split_series_key(self, key):
        raise NotImplementedError("split_series_key not implemented")

    def series_key_to_sensor_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return sensor

    def series_key_to_device_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return dev

