# Script to export TempoDB data to TempoIQ
# Relies on TempoDB python client v1.0: https://github.com/tempodb/tempodb-python/tree/v1.0

import gevent
from gevent.queue import JoinableQueue
import gevent.monkey
gevent.monkey.patch_all()  # Do this before other imports

import time
import datetime
import os
import re
import json
import uuid
import tempoiq.protocol
import logging
from tempodb.client import Client as TDBClient
from tempoiq.client import Client as TIQClient
from tempoiq.endpoint import HTTPEndpoint
from tempoiq.protocol.encoder import WriteEncoder, CreateEncoder
import tempoiq.response
from threading import Lock


class Migrator:
    def __init__(self, scheme, create_devices=True,
                 write_data=True,
                 start_date="2000-01-01T00:00:00Z",
                 end_date="2014-12-31T00:00:00Z",
                 pool_size=3):
        self.scheme = scheme
        self.create_devices = create_devices
        self.should_write_data = write_data
        self.start_date = start_date
        self.end_date = end_date
        self.tdb = TDBClient(scheme.db_key, scheme.db_key,
                             scheme.db_secret,
                             base_url=scheme.db_baseurl)

        iq_endpoint = HTTPEndpoint(scheme.iq_baseurl,
                                   scheme.iq_key,
                                   scheme.iq_secret)
        self.tiq = TIQClient(iq_endpoint)
        self.queue = JoinableQueue()
        self.lock = Lock()
        self.dp_count = 0
        self.req_count = 0
        self.dp_reset = time.time()
        for i in range(pool_size):
            gevent.spawn(self.worker)

    def worker(self):
        while True:
            series = self.queue.get()
            try:
                self.migrate_series(series)
            finally:
                self.queue.task_done()

    def migrate_all_series(self, start_key="", limit=None):
        start_time = time.time()

        (keys, tags, attrs) = self.scheme.identity_series_filter()
        series_set = self.tdb.list_series(keys, tags, attrs)

        # Keep our own state of whether we passed the resume point, so we don't
        # need to assume client and server sort strings the same.
        found_first_series = False

        series_count = 0

        for series in series_set:
            if not found_first_series and series.key < start_key:
                continue
            else:
                found_first_series = True

            if limit and series_count >= limit:
                print("Reached limit of %d devices, stopping." % (limit))
                break

            if self.scheme.identity_series_client_filter(series):
                # If the series looks like an identity series,
                # queue it to be processed by the threadpool
                self.queue.put(series)
                series_count += 1

        self.queue.join()

        end_time = time.time()
        print("Exporting {} devices took {} seconds".format(series_count, end_time - start_time))

    def migrate_series(self, series):
        print("  Beginning to migrate series: %s" % (series.key))
        error = False
        try:
            if self.create_devices:
                error = self.create_device(series)

            if self.should_write_data and not error:
                error = self.write_data(series)
        except Exception, e:
            logging.exception(e)
            error = True

        if not error:
            print("COMPLETED migrating for series %s" % (series.key))
        else:
            print("ERROR migrating series %s" % (series.key))

    def create_device(self, series):
        (keys, tags, attrs) = self.scheme.series_to_filter(series)

        dev_series = []
        device_key = self.scheme.series_to_device_key(series)

        series_set = self.tdb.list_series(keys, tags, attrs)
        for ser in series_set:
            if self.scheme.series_client_filter(ser, series):
                dev_series.append(ser)

        if len(dev_series) == 0:
            print("No series found for filter: " + series.key)
            return True

        device = self.scheme.all_series_to_device(dev_series)
        response = self.tiq.create_device(device)
        if response.successful != tempoiq.response.SUCCESS:
            if "A device with that key already exists" in response.body:
                print("Device already exists: %s" % (device.key))
            else:
                print("ERROR creating device: %s reason: (%d) %s"
                      % (device.key, response.status, response.reason))
                print("   " + response.body)
                print("   " + json.dumps(device, default=CreateEncoder().default))
                return True
        return False

    def write_data(self, series):
        (keys, tags, attrs) = self.scheme.series_to_filter(series)
        device_key = self.scheme.series_to_device_key(series)

        db_data = self.tdb.read_multi(keys=keys, tags=tags, attrs=attrs,
                                      start=self.start_date, end=self.end_date)
        device_data = {}
        count = 0
        retries = 0

        for point in db_data:
            for (s_key, val) in point.v.items():
                sensor = self.scheme.series_key_to_sensor_key(s_key)
                sensor_data = device_data.setdefault(sensor, [])
                sensor_data.append({"t": point.t, "v": val})
                count += 1

            if count > 100:
                write_request = {device_key: device_data}
                self.write_with_retry(write_request, 3)
                self.increment_counter(count)
                count = 0
                device_data = {}

        if count > 0:
            write_request = {device_key: device_data}
            self.write_with_retry(write_request, 3)
            self.increment_counter(count)

        return False

    def increment_counter(self, count):
        self.lock.acquire()
        now = time.time()
        self.req_count += 1
        self.dp_count += count

        if (now - self.dp_reset > 10):
            dpsec = self.dp_count / (now - self.dp_reset)
            reqsec = self.req_count / (now - self.dp_reset)
            print("{0} Write throughput: {1:.2f} dp/s, {2:.2f} req/sec"
                  .format(datetime.datetime.now(), dpsec, reqsec))
            self.dp_reset = now
            self.dp_count = 0
            self.req_count = 0

        self.lock.release()

    def write_with_retry(self, write_request, retries):
        try:
            res = self.tiq.write(write_request)
        except Exception, e:
            print("ERROR with request: --->")
            print(json.dumps(write_request, default=WriteEncoder().default))
            raise e

        if res.successful != tempoiq.response.SUCCESS:
            print("ERROR writing data! Reason: %s."
                  % (res.body))
            print(json.dumps(write_request, default=WriteEncoder().default))

            if retries > 0:
                print("Retrying")
                return self.write_with_retry(write_request, retries - 1)
            else:
                print("No more retries! Lost data!")
                return True
        return False



