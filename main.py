import pandas as pd
import numpy as np
import redis
from flask import Flask,request


app = Flask(__name__)

r=redis.Redis(host='127.0.0.1', port=6379)

if r.ping():
    print("pong")



def get_csv_data_asc_sts():
    df = pd.read_csv('raw_data.csv', low_memory=False)
    df.sort_values(by=['sts'], inplace=True, ascending=True)
    return df



def push_recent_data_to_redis():
    df = get_csv_data_asc_sts()
    for ind in df.index:
        device_fk_id = int(df['device_fk_id'][ind])
        latitude = float(df['latitude'][ind])
        longitude = float(df['longitude'][ind])
        time_stamp = str(df['time_stamp'][ind])
        sts = str(df['sts'][ind])
        speed = int(df['speed'][ind])

        found_device_fk_id = r.hmget(f"device_fk_id_{device_fk_id}", "device_fk_id")
        found_time_stamp = r.hmget(f"device_fk_id_{device_fk_id}", "time_stamp")

        if found_device_fk_id[0] and found_time_stamp[0]:
            time_stamp=max([time_stamp,found_time_stamp[0].decode('utf-8')])

        r.hset(f"device_fk_id_{device_fk_id}", \
               mapping={"device_fk_id": device_fk_id, "latitude": latitude, "longitude": longitude, \
                        "time_stamp": time_stamp, "sts": sts, "speed": speed})


def get_recent_device_redis(device_fk_id):
    inner_fields = dict()
    final = dict()
    count = 0
    inner_field_keys = ["device_fk_id","latitude","longitude","time_stamp","sts","speed"]

    found_device_fk_id = r.hmget(f"device_fk_id_{device_fk_id}", "device_fk_id")
    if found_device_fk_id[0] is not None:
        row = list(r.hmget(f"device_fk_id_{device_fk_id}", "device_fk_id", "latitude", "longitude", "time_stamp", "sts",
                           "speed"))
        row = [x.decode('utf-8') for x in row]
        for ele in row:
            inner_fields.update({inner_field_keys[count]: ele})
            count+=1

        final.update({f"device_details": inner_fields})

    return final


def get_recent_location_redis(device_fk_id):
    inner_fields = dict()
    final = dict()
    count = 0
    inner_field_keys = ["device_fk_id","latitude","longitude","time_stamp","sts","speed"]

    found_device_fk_id = r.hmget(f"device_fk_id_{device_fk_id}", "device_fk_id")
    if found_device_fk_id[0] is not None:
        row = list(r.hmget(f"device_fk_id_{device_fk_id}", "device_fk_id", "latitude", "longitude", "time_stamp", "sts",
                           "speed"))
        row = [x.decode('utf-8') for x in row]
        for ele in row:
            inner_fields.update({inner_field_keys[count]: ele})
            count+=1

        final.update({f"location_details": inner_fields})

    return final


def get_recent_location_time_redis(device_fk_id):
    inner_fields = dict()
    final = dict()
    count = 0
    inner_field_keys = ["device_fk_id","latitude","longitude","time_stamp","sts","speed"]

    found_device_fk_id = r.hmget(f"device_fk_id_{device_fk_id}", "device_fk_id")
    if found_device_fk_id[0] is not None:
        row = list(r.hmget(f"device_fk_id_{device_fk_id}", "device_fk_id", "latitude", "longitude", "time_stamp", "sts",
                           "speed"))
        row = [x.decode('utf-8') for x in row]
        for ele in row:
            inner_fields.update({inner_field_keys[count]: ele})
            count+=1

        final.update({f"location_time_details": inner_fields})

    return final


@app.route('/device_info')
def get_device_info():
    # push_recent_data_to_redis()
    args = request.args
    return get_recent_device_redis(args.get('device_fk_id'))

@app.route('/location_info')
def get_location_info():
    # push_recent_data_to_redis()
    args = request.args
    return get_recent_location_redis(args.get('device_fk_id'))

@app.route('/location_time_info')
def get_location_time_info():
    # push_recent_data_to_redis()
    args = request.args
    return get_recent_location_time_redis(args.get('device_fk_id'))

if __name__ == '__main__':
    app.run()  # run our Flask app


# push_recent_data_to_redis()
# get_recent_data_redis(6888)




