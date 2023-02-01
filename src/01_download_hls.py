
# https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/05_Data_Access_Direct_S3.html
# Instructions for getting access to the NASA data: https://urs.earthdata.nasa.gov/documentation/for_users/data_access/curl_and_wget

# imports
from datetime import datetime, timedelta
import os
import requests
import boto3
import rasterio as rio
from rasterio.session import AWSSession

import pandas as pd
import json
from loguru import logger
import multiprocessing

START_IDX = 0 # index starts from 0
END_IDX = 2  # None means to finish until end (index also starts at 0)
FAIL_LOG_FP = "../data/failure_indices.txt"
DATA_DIR = "../data"                            # where data is stored (csv, json reference files)
DATE_STR_FMT = '%Y-%m-%d'
OUT_DIR = "/Volumes/R Sandisk SSD/hls_tmp"    # folder where data will be downloaded to
MAX_THREADS = 4     # when running multiprocessing, this is the max number of concurrently running processes

ssc_json_path = f'{DATA_DIR}/ssc_sample_2573.json'
# aqsat_path = f'{DATA_DIR}/Aqusat_TSS_v1.csv'
aqsat_path = f'{DATA_DIR}/filtered_data.csv'        # filtered data from 00_filter_csv.py
s3_cred_endpoint = 'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials'


def get_temp_creds():
    temp_creds_url = s3_cred_endpoint
    return requests.get(temp_creds_url).json()


def load_data(csv_fp, json_fp):
    # TODO: possible issue is if csv_data or json_data is too large to be loaded to memory
    logger.debug(f"Loading data from {csv_fp} and {json_fp}")
    csv_data = pd.read_csv(csv_fp)
    with open(json_fp, 'r') as f:
        json_data = json.load(f)
    logger.info(f"Loaded json: {len(json_data)} and csv:{csv_data.shape}")
    
    return csv_data, json_data


def save_hls_data(hls_links, fp_dir_out):
    for url in hls_links:
        # TODO: use time from aqsat to filter the links (i.e., get data closest to aqsat `date_utc`)
        # TODO: need to add time in json file if needed (since there can be multiple tiles in a day)
        logger.debug(f"Getting data from: {url}")

        # send a HTTP request and save
        fp_out = os.path.join(fp_dir_out, url.split("/")[-1])
        r = requests.get(url) # create HTTP response object
        with open(fp_out,'wb') as f:
            f.write(r.content)


def get_hls_links(json_data, json_key, date, fail_log_fp=FAIL_LOG_FP):
    try:
        hls_links = json_data[json_key][date]
    except KeyError:
        logger.error(f"KeyError: {json_key} not found in `json_data`")
        with open(fail_log_fp, "a") as fp:
            fp.write(f"[{json_key}, {date}]: not found in `json_data`,\n")
        return [] # return no data
    return hls_links


def process_row(row, json_data, date_str_fmt=DATE_STR_FMT, out_dir=OUT_DIR):
    # NOTE: this is the function that can be called in parallel
    lat, long = row["lat"], row["long"]
    json_key = str(long) +','+ str(lat)

    # Get all dates, +/-1 day from listed day (including original date)
    date_orig = row['date']
    date_obj = datetime.strptime(date_orig, date_str_fmt).date()
    dates = [
        datetime.strftime(date_obj - timedelta(x), date_str_fmt)
        for x in [-1,0,1]
    ]
    
    for date in dates:
        if not os.path.exists(os.path.join(out_dir, json_key)):          # make folder with lat,long as name
            os.makedirs(os.path.join(out_dir, json_key))
        if not os.path.exists(os.path.join(out_dir, json_key, date)):    # make folder with date as name
            os.makedirs(os.path.join(out_dir, json_key, date))
        hls_links = get_hls_links(json_data, json_key, date)

        fp_dir_out = os.path.join(out_dir, json_key, date)
        save_hls_data(hls_links, fp_dir_out)
    return f"[{json_key}, {date}]"


def process_csv_data(aqsat, json_data, out_dir=OUT_DIR, max_threads=MAX_THREADS):
    # Parallel processing sample
    for n in range((len(aqsat)//max_threads)+1):
        start_idx = max_threads*n
        end_idx = start_idx+max_threads
        subset = aqsat.iloc[start_idx:end_idx]
        
        pool = multiprocessing.Pool()
        result_async = [
            pool.apply_async(process_row, args=(subset.iloc[i], json_data))
            for i in range(len(subset))
        ]
        results = [r.get() for r in result_async]
        logger.info(f"Finished processing: {results}")


if __name__ == "__main__":
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)
    aqsat, json_data = load_data(aqsat_path, ssc_json_path)

    # Load credentials for environment
    temp_creds_req = get_temp_creds()
    session = boto3.Session(
        aws_access_key_id=temp_creds_req['accessKeyId'], 
        aws_secret_access_key=temp_creds_req['secretAccessKey'],
        aws_session_token=temp_creds_req['sessionToken'],
        region_name='us-west-2'
    )
    rio_env = rio.Env(AWSSession(session),
        GDAL_DISABLE_READDIR_ON_OPEN='EMPTY_DIR',
        GDAL_HTTP_COOKIEFILE=os.path.expanduser('~/cookies.txt'),
        GDAL_HTTP_COOKIEJAR=os.path.expanduser('~/cookies.txt')
    )
    rio_env.__enter__()

    process_csv_data(aqsat.iloc[START_IDX: END_IDX+1], json_data, OUT_DIR)
