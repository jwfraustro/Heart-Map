import csv
import json
from datetime import datetime
from queue import Queue
from threading import Thread

heart_file_1 = './data/heart_rates.csv'
heart_file_2 = './data/heart_rates_processed.csv'

loc_hist_file = './data/Location History.json'
loc_hist_processed = './data/loc_history.csv'


def process_heart_rates():
    print("Processing heart rates...")
    heart_data = []

    with open(heart_file_1, 'r') as r:
        reader = csv.reader(r)
        next(reader)
        for row in reader:
            d_time = row[1]
            bpm = row[2]
            heart_data.append({'d_time': d_time, 'bpm': bpm})

    for entry in heart_data:
        d_time = entry['d_time']
        dt = datetime.strptime(d_time, '%d-%b-%Y %H:%M')
        time_stamp = dt.timestamp()
        entry['time'] = int(time_stamp)

    with open(heart_file_2, 'w', newline='') as w:
        writer = csv.writer(w)
        for entry in heart_data:
            writer.writerow([entry['time'], entry['d_time'], entry['bpm']])

    print("Heart rate records:", len(heart_data))
    return heart_data


def process_loc_history():
    print("Processing location history...")
    with open(loc_hist_file, 'r') as r:
        loc_data = json.load(r)

    loc_data = loc_data['locations']

    loc_data_new = []

    for entry in loc_data:
        time = float(entry['timestampMs']) / 1000
        lat = entry['latitudeE7'] / 1E7
        long = entry['longitudeE7'] / 1E7

        loc_data_new.append({'time': time, 'lat': lat, 'long': long})

    with open(loc_hist_processed, 'w', newline='') as w:
        writer = csv.writer(w)
        for entry in loc_data_new:
            writer.writerow([entry['time'], entry['lat'], entry['long']])

    print("Location history records:", len(loc_data_new))
    return loc_data_new


def compare_heart_loc(heart_data, loc_data):
    matches = []

    for i, entry in enumerate(reversed(heart_data)):
        print("Comparing heart entry %s/%s" % (str(i), str(len(heart_data))))
        heart_time = entry['time']
        loc_match = None
        best_time = 1000

        for location in reversed(loc_data):

            time_diff = abs(heart_time - location['time'])

            if time_diff <= best_time:
                best_time = time_diff
                loc_match = location

        if loc_match:
            print("Found match!")
            matches.append([entry['time'], loc_match['time'], loc_match['lat'], loc_match['long'], entry['bpm']])

    return matches


heart_data = process_heart_rates()
loc_data = process_loc_history()
heart_loc_matched = compare_heart_loc(heart_data, loc_data)
input()