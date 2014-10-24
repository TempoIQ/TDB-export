# Script to export TempoDB data. 
# Relies on TempoDB python client v1.0: https://github.com/tempodb/tempodb-python/tree/v1.0
 
import datetime
import os
import re
import json
import uuid
from tempodb.client import Client
 
EXPORT_DIR = "export"
API_KEY = "your-key-here"
API_SECRET = "your-secret-here"
 
 
# UTC datetime range from which to export data
START_DATE = datetime.datetime(2013, 1, 1)
END_DATE = datetime.datetime.utcnow()
 
 
class Exporter:
    def __init__(self):
        self.client = Client(API_KEY, API_KEY, API_SECRET)
        self.series_filename = os.path.join(EXPORT_DIR, "series_info")
 
        if not os.path.exists(EXPORT_DIR):
            print "Making export directory"
            os.makedirs(EXPORT_DIR)
 
    def export_metadata(self):
        if os.path.isfile(self.series_filename):
            print "series_info exists, skipping series discovery"
            return
 
        print "Exporting series metadata"
        all_series = self.client.list_series()
 
        with open(self.series_filename, 'w') as outfile:
            for series in all_series:
                line = self.series_to_string(series) + "\n"
                outfile.write(line.encode("utf-8"))
 
    def series_to_string(self, series):
        local_id = uuid.uuid4()
        j = {"uuid": str(local_id)}    # Generate series UUID since keys
        for p in series.properties:    # could be inconvenient filenames
            j[p] = getattr(series, p)
 
        return json.dumps(j, ensure_ascii=False)
 
    def export_all_series(self):
        if not os.path.isfile(self.series_filename):
            print "ERROR: No series_info file found, can't export series data"
            return
 
        with open(self.series_filename, 'r') as series_list:
            for text in series_list:
                series = json.loads(text)
                self.export_single_series(series.get('key'),
                                          series.get('uuid'))
 
    def export_single_series(self, key, uuid):
        filename = os.path.join(EXPORT_DIR, uuid + ".csv")
 
        if os.path.isfile(filename):
            print "Data file exists for series " + key + ", skipping"
            return
 
        print "Exporting series " + key + " to " + filename
        response = self.client.read_data(key=key.encode('utf-8'), start=START_DATE, end=END_DATE)
 
        with open(filename, 'w') as outfile:
            for dp in response:
                line = dp.t.isoformat() + "," + str(dp.v) + "\n"
                outfile.write(line)
 
 
def main():
    exporter = Exporter()
    exporter.export_metadata()
    exporter.export_all_series()
 
 
if __name__ == "__main__":
    main()
