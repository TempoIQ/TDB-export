# Script to export TempoDB data to TempoIQ
# Relies on TempoDB python client v1.0: https://github.com/tempodb/tempodb-python/tree/v1.0

import time
import datetime
import os
import re
import json
import uuid
import tempoiq.protocol
from tempodb.client import Client as TDBClient
from tempoiq.client import Client as TIQClient
from tempoiq.endpoint import HTTPEndpoint
from tempoiq.protocol.encoder import WriteEncoder, CreateEncoder
import tempoiq.response
from threading import Thread
from Queue import Queue


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
                 start_date="2000-01-01T00:00:00Z",
                 end_date="2014-12-31T00:00:00Z",
                 pool_size=3):
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
        self.pool = ThreadPool(pool_size)

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
                self.pool.add_task(self.migrate_series, series)
                series_count += 1

        self.pool.wait_completion()

        end_time = time.time()
        print("Exporting {} devices took {} seconds".format(series_count, end_time - start_time))

    def migrate_series(self, series):
        print("  Beginning to migrate series: %s" % (series.key))

        (keys, tags, attrs) = self.scheme.series_to_filter(series)
        device_key = self.scheme.series_key_to_device_key(series.key)

        if self.create_devices:
            series_set = self.tdb.list_series(keys, tags, attrs)
            dev_series = []
            for series in series_set:
                dev_series.append(series)

            device = self.scheme.all_series_to_device(dev_series)
            response = self.tiq.create_device(device)
            if response.successful != tempoiq.response.SUCCESS:
                print("ERROR creating device: %s reason: (%d) %s"
                      % (device.key, response.status, response.reason))
                print("   " + response.body)
                print("   " + json.dumps(device, default=CreateEncoder().default))
                return

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

            if count > 50:
                write_request = {device_key: device_data}
                self.write_with_retry(write_request, 3)
                count = 0
                device_data = {}

            count += 1

        print("COMPLETED migrating device %s" % (device_key))

    def write_with_retry(self, write_request, retries):
        res = self.tiq.write(write_request)
        if res.successful != tempoiq.response.SUCCESS:
            print("ERROR writing data! Reason: %s."
                  % (res.body))
            print(json.dumps(write_request, default=WriteEncoder().default))

            if retries > 0:
                print("Retrying")
                self.write_with_retry(write_request, retries - 1)
            else:
                print("No more retries! Lost data!")



