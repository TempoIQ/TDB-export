import re
from tempoiq.protocol import Device, Sensor
from default import MigrationScheme


class Example(MigrationScheme):
    """Migration scheme for the data model example in
    http://tempoiq.github.io/docs/guides/tempodb-transition.html#id3 """
    db_key = ""
    db_secret = ""
    db_baseurl = "https://api.tempo-db.com/v1/"

    iq_key = ""
    iq_secret = ""
    iq_baseurl = "https://your-host.backend.tempoiq.com"

    def __init__(self):
        # Sensors that need to be added to each device
        self.sensors = []

        for sens in ["energy", "voltage"]:
            self.sensors.append(Sensor(sens))

    def identity_series_filter(self):
        """Filter that will return one TempoDB series for every device that
        should be created. Filter is a (keys, tags, attrs) tuple."""
        return (None, ["energy"], {})

    def series_to_filter(self, series):
        """Given a TempoDB identity series object, return a filter for all
        series that should be included in the same device."""
        meter = series.attributes['meter']

        return (None, [], {"meter": meter})

    def all_series_to_device(self, series_list):
        """Given a list of all TempoDB series belonging to the same device, return
        the corresponding IQ device object"""
        attributes = {}

        series = series_list[0]    # Doesn't matter which one

        # Device attributes
        for key in ["meter", "region", "status"]:
            attributes[key] = series.attributes[key]

        device = Device(self.series_to_device_key(series.key),
                        attributes=attributes, sensors=self.sensors)

        return device

    def split_series_key(self, key):
        """Take a series key string, and return a tuple of
        (devicekey, sensorkey)."""
        match = re.match(r"(meter:.+)\..*\.(.+)\.$", key)

        if match:
            return (match.group(1), match.group(2))
        else:
            return ("unknown", "unknown")

    def series_key_to_sensor_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return sensor

    def series_key_to_device_key(self, series_key):
        (dev, sensor) = self.split_series_key(series_key)
        return dev
