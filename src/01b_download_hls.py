
# https://nasa-openscapes.github.io/2021-Cloud-Hackathon/tutorials/05_Data_Access_Direct_S3.html
# Instructions for getting access to the NASA data: https://urs.earthdata.nasa.gov/documentation/for_users/data_access/curl_and_wget
# NOTE: This script loads data into memory and modifies to only keep a certain size of the tile from the lat/lon

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
import pyproj
import rioxarray
import numpy as np
import cv2
import utm
from matplotlib import pyplot as plt

# For CV samples of saved images
NAN_TO_NUM_VAL = 0  # what to put in nan pixels
PADDING_VAL = 200   # number of pixels to pad around the image in case lat/lon is at border
NUM_PX_OBS = 100    # (creates 6kmx6km stuff) number of pixels to extend per side for cropped observation (Note: 1px=30m)

START_IDX = 0 # index starts from 0
END_IDX = 2  # None means to finish until end (index also starts at 0)
FAIL_LOG_FP = "../data/failure_indices.txt"
DATA_DIR = "../data"                            # where data is stored (csv, json reference files)
DATE_STR_FMT = '%Y-%m-%d'
LAT_COL = "lat"     # column name for latitude
LON_COL = "long"    # column name for longitude
SITE_ID_COL = "SiteID"  # column name for Site ID
# OUT_DIR = "/Volumes/R Sandisk SSD/hls_tmp"    # folder where data will be downloaded to
OUT_DIR = "../hls_img_data"
MAX_THREADS = 4     # when running multiprocessing, this is the max number of concurrently running processes

ssc_json_path = f'{DATA_DIR}/checkpoint_197000.json'
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

def save_cropped_hls(fp_out, url, lat, lon):
    global PADDING_VAL, NUM_PX_OBS
    band_name = url.split("/")[-1].split(".")[-2]
    # Load data from NASA link
    rx_data = rioxarray.open_rasterio(url, chuncks=True, masked=True).squeeze() # masked=True sets pixels with no values to nan

    # Transform coordinate system for lat/lon
    src = rx_data.rio
    latlon_equiv = utm.from_latlon(lat, lon)    # tile projection is UTM from https://lpdaac.usgs.gov/documents/1326/HLS_User_Guide_V2.pdf
    l_bd, b_bd, r_bd, t_bd = src.bounds()    #left, bottom, right, top
    x_res, y_res = src.resolution()    # x_res, y_res
    # TODO: check resolutions for other bands and where this processing is compatible

    # Compute coordinates in image for the lat/lon
    img_x, img_y = int(abs(latlon_equiv[0]-l_bd)/(abs(x_res))), int(abs(t_bd-latlon_equiv[1])/(abs(y_res)))

    # Get array values
    src_val = rx_data.values

    # Crop, normalize, and save image
    padded_src = np.pad(src_val, PADDING_VAL, mode="reflect")   # padding in case the lat/lon is at the border
    cropped_img = padded_src[
        PADDING_VAL+img_y-NUM_PX_OBS: PADDING_VAL+img_y+NUM_PX_OBS,
        PADDING_VAL+img_x-NUM_PX_OBS: PADDING_VAL+img_x+NUM_PX_OBS,
    ]
    if band_name.lower() != "fmask": # don't normalize fmask
        logger.debug(f"Normalizing for {band_name}")
        cropped_img = ((cropped_img-np.nanmin(cropped_img))/(np.nanmax(cropped_img)-np.nanmin(cropped_img)))*255 # normalize
    cropped_img = np.nan_to_num(cropped_img, nan=NAN_TO_NUM_VAL)
    cropped_img = cropped_img.astype(int)
    img_fp = fp_out.replace(".tif", ".png")
    logger.debug(f"Saving to: {img_fp} [{img_x}, {img_y}][{latlon_equiv}], [{np.nanmax(cropped_img)}, {np.nanmin(cropped_img)}]")
    cv2.imwrite(img_fp, cropped_img)
    # plt.imshow(cropped_img)
    # plt.scatter(NUM_PX_OBS, NUM_PX_OBS, marker='x', c="red")
    # plt.savefig(img_fp.replace(".png", ".jpg"))
    # plt.close()
    

def save_hls_data(hls_links, fp_dir_out, lat, lon):
    hls_links = hls_links + [
        f"{hls_links[-1][:-7]}Fmask.tif",
        f"{hls_links[-1][:-7]}SAA.tif",
        f"{hls_links[-1][:-7]}SZA.tif",
        f"{hls_links[-1][:-7]}VAA.tif",
        f"{hls_links[-1][:-7]}VZA.tif",
        f"{hls_links[-1][:-7]}json",    # size and checksum of each file
        f"{hls_links[-1][:-7]}cmr.xml", # metadata file
        # f"{hls_links[-1][:-7]}jpg", # natural-color browse image
    ]
    for url in hls_links:
        # TODO: use time from aqsat to filter the links (i.e., get data closest to aqsat `date_utc`)
        # TODO: need to add time in json file if needed (since there can be multiple tiles in a day)
        logger.debug(f"Getting data from: {url}, lat,lon: {lat},{lon}")

        # send a HTTP request and save
        fp_out = os.path.join(fp_dir_out, url.split("/")[-1])
        if url.endswith(".tif"):
            save_cropped_hls(fp_out, url, lat, lon)   # Save cropped version
        else:   # for json and xml files, no need to preprocess
            r = requests.get(url) # create HTTP response object
            with open(fp_out,'wb') as f:
                f.write(r.content)


def get_hls_links(json_data, site_id, date, fail_log_fp=FAIL_LOG_FP):
    try:
        hls_links = json_data[site_id]["dates"][date]
        if not isinstance(hls_links, list):
            logger.error(f"KeyError: [{site_id}, {date}] not found in `json_data`")
            return []    
    except KeyError:
        logger.error(f"KeyError: [{site_id}, {date}] not found in `json_data`")
        with open(fail_log_fp, "a") as fp:
            fp.write(f"[{site_id}, {date}]: not found in `json_data`,\n")
        return [] # return no data
    return hls_links


def process_row(row, json_data, out_dir):
    # NOTE: this is the function that can be called in parallel
    site_id = row[SITE_ID_COL]

    # Get all dates, +/-1 day from listed day (including original date)
    date = row['date']
    lat, lon = row[LAT_COL], row[LON_COL]
    
    if not os.path.exists(os.path.join(out_dir, site_id)):          # make folder with lat,long as name
        os.makedirs(os.path.join(out_dir, site_id))
    if not os.path.exists(os.path.join(out_dir, site_id, date)):    # make folder with date as name
        os.makedirs(os.path.join(out_dir, site_id, date))
    hls_links = get_hls_links(json_data, site_id, date)

    fp_dir_out = os.path.join(out_dir, site_id, date)
    save_hls_data(hls_links, fp_dir_out, lat, lon)
    return f"[{site_id}, {date}]"


def process_csv_data(aqsat, json_data, out_dir, max_threads):
    # Parallel processing sample
    for n in range((len(aqsat)//max_threads)+1):
        start_idx = max_threads*n
        end_idx = start_idx+max_threads
        subset = aqsat.iloc[start_idx:end_idx]
        
        pool = multiprocessing.Pool()
        result_async = [
            pool.apply_async(process_row, args=(subset.iloc[i], json_data, out_dir))
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

    process_csv_data(aqsat.iloc[START_IDX: END_IDX+1], json_data, OUT_DIR, MAX_THREADS)
