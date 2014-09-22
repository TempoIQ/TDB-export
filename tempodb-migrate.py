# Script to export TempoDB data to TempoIQ
# Relies on TempoDB python client v1.0: https://github.com/tempodb/tempodb-python/tree/v1.0

import datetime
import os
import re
import json
import uuid
import tempoiq.protocol
from tempodb.client import Client as TDBClient
from tempoiq.client import Client as TIQClient
from tempoiq.endpoint import HTTPEndpoint
from tempoiq.protocol.encoder import WriteEncoder
import tempoiq.response
from threading import Thread
from Queue import Queue


class MigrationScheme:
    db_key = "2d5f744a72c8474e8e7d47f5863a2f06"
    db_secret = "d6ef0208e2764d96af8d6daa72f055e3"
    db_baseurl = "https://api.tempo-db.com/v1/"

    iq_key = "522b4943a0014e75a43b10155a3732de"
    iq_secret = "590c54d4670b472dacb3555c4b6c97fc"
    iq_baseurl = "https://sandbox-matt.backend.tempoiq.com"

    # Filter that will return one TempoDB series for every device that
    # should be created. Filter is a (keys, tags, attrs) tuple.
    def series_filter(self):
        return (None, [], {"project": "perftest1"})

    # Given a TempoDB series object, return a TempoIQ device with all relevant
    # sensors attached.
    # TODO: do we need all series or can we hardcode sensors like this?
    def series_to_device(self, series):
        #sensor_keys = ["ApparentTemperature", "ApparentTemperatureMax",
        #        "ApparentTemperatureMaxTime", "ApparentTemperatureMin",
        #        "ApparentTemperatureMinTime", "CloudCover", "DewPoint",
        #        "Humidity", "Ozone", "PrecipIntensity", "PrecipIntensityMax"]
        #sensors = []

        #for sensor_key in sensor_keys:
        #    sensor = tempoiq.protocol.Sensor(sensor_key)
        #    sensors.append(sensor)

        device = tempoiq.protocol.Device(
                    self.series_key_to_device_key(series.key))

        device.sensors = [tempoiq.protocol.Sensor("vals")]
        device.attributes["type"] = "perftest1"
        device.attributes["devID"] = self.get_id_from_key(series.key)

        return device

    def get_id_from_key(self, series_key):
        return re.search(r".*\.([\w\d]+)", series_key).group(1)

    # Given a TempoDB series object, return a filter for all
    # series that should be included in the same device.
    def series_to_filter(self, series):
        #lat = series.attributes.get("Latitude", "undef")
        #lng = series.attributes.get("Longitude", "undef")

        return ([series.key], [], {})

    def series_key_to_sensor_key(self, series_key):
        #sensor = re.match(r"Type:(\w+)", series_key)
        # TODO: better error handling
        return "vals"

    def series_key_to_device_key(self, series):
        return "perf-series.%s" % self.get_id_from_key(series)


class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception, e:
                print e
            self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()


class Migrator:
    def __init__(self, scheme, create_devices=True,
                 start_date="2010-01-01T00:00:00Z", end_date="2014-12-31T00:00:00Z"):
        self.scheme = scheme
        self.create_devices = create_devices
        self.start_date = start_date
        self.end_date = end_date
        self.tdb = TDBClient(scheme.db_key, scheme.db_key,
                             scheme.db_secret,
                             base_url=scheme.db_baseurl)

        iq_endpoint = HTTPEndpoint(scheme.iq_baseurl,
                                   scheme.iq_key,
                                   scheme.iq_secret)
        self.tiq = TIQClient(iq_endpoint)
        self.pool = ThreadPool(3)

    def migrate_all_series(self, start_key=""):
        (keys, tags, attrs) = self.scheme.series_filter()
        series_set = self.tdb.list_series(keys, tags, attrs)

        # Keep our own state of whether we passed the resume point, so we don't
        # need to assume client and server sort strings the same.
        found_first_series = False

        for series in series_set:
            if not found_first_series and series.key < start_key:
                print("Skipping series %s" % (series.key))
                continue
            else:
                found_first_series = True

            # Queue each series to be processed by the threadpool
            self.pool.add_task(self.migrate_series, series)

    def migrate_series(self, series):
        print("  Beginning to migrate series: %s" % (series.key))

        device = self.scheme.series_to_device(series)

        if self.create_devices:
            response = self.tiq.create_device(device)
            if response.successful != tempoiq.response.SUCCESS:
                print("ERROR creating device: %s reason: (%d) %s"
                      % (device.key, response.status, response.reason))
                print("   " + response.body)
                return

        (keys, tags, attrs) = self.scheme.series_to_filter(series)

        db_data = self.tdb.read_multi(keys=keys, tags=tags, attrs=attrs,
                                      start=self.start_date, end=self.end_date)
        device_data = {}
        count = 0

        for point in db_data:
            for (s_key, val) in point.v.items():
                sensor = self.scheme.series_key_to_sensor_key(s_key)
                sensor_data = device_data.setdefault(sensor, [])
                sensor_data.append({"t": point.t, "v": val})

            if count > 10:
                write_request = {device.key: device_data}
                res = self.tiq.write(write_request)
                if res.successful != tempoiq.response.SUCCESS:
                    print("ERROR writing data for device: %s ending at %s. Reason: %s"
                          % (device.key, point.t, res.body))
                    print(json.dumps(write_request, default=WriteEncoder().default))
                    return

                count = 0
                device_data = {}

            count += 1

        print("COMPLETED migrating device %s" % (device.key))


def main():
    migrator = Migrator(MigrationScheme(),
                        create_devices=False)
    migrator.migrate_all_series(start_key="project:perftest1.11")


if __name__ == "__main__":
    main()
