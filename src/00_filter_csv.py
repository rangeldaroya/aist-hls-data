"""
This script is meant to filter the csv file to those that have corresponding entries in the json file
"""
from datetime import datetime, timedelta
import pandas as pd
import json
from loguru import logger

# Preset variables defined here
DATA_DIR = "../data"
JSON_FP = f"{DATA_DIR}/ssc_sample_2573.json"    # reference file for links
SSC_DATA_FP = f"{DATA_DIR}/Aqusat_TSS_v1.csv"   # location of csv
OUT_FILTERED_CSV_FP = f"{DATA_DIR}/filtered_data.csv"    # fp where filtered csv will be saved to

DATE_STR_FMT = '%Y-%m-%d'
LAT_COL = "lat"     # column name for latitude
LON_COL = "long"    # column name for longitude


def load_data(csv_fp, json_fp):
    # TODO: possible issue is if csv_data or json_data is too large to be loaded to memory
    logger.debug(f"Loading data from {csv_fp} and {json_fp}")
    csv_data = pd.read_csv(csv_fp)
    with open(json_fp, 'r') as f:
        json_data = json.load(f)
    logger.info(f"Loaded json: {len(json_data)} and csv:{csv_data.shape}")
    
    return csv_data, json_data


def get_matched_rows(csv_data, json_data, lat_col=LAT_COL, lon_col=LON_COL, date_str_fmt=DATE_STR_FMT):
    matched_idx = []  # row indices in csv file that have a match
    for idx in range(len(csv_data)):
        row = csv_data.iloc[idx]

        lat, long = row[lat_col], row[lon_col]
        json_key = str(long) +','+ str(lat)

        # Get all dates, +/-1 day from listed day (including original date)
        date_orig = row['date']
        date_obj = datetime.strptime(date_orig, date_str_fmt).date()
        dates = [
            datetime.strftime(date_obj - timedelta(x), date_str_fmt)
            for x in [-1,0,1]
        ]
        for date in dates:
            try:
                _ = json_data[json_key][date]
                matched_idx.append(idx)
                break
            except KeyError:
                logger.error(f"[{json_key}, {date}] not found in `json_data`")
    return matched_idx


if __name__ == "__main__":
    csv_data, json_data = load_data(SSC_DATA_FP, JSON_FP)
    matched_idx = get_matched_rows(csv_data, json_data, lat_col=LAT_COL, lon_col=LON_COL, date_str_fmt=DATE_STR_FMT)

    filtered_csv = csv_data.iloc[matched_idx]
    logger.info(f"Filtered csv from {csv_data.shape} to {filtered_csv.shape}")
    filtered_csv.to_csv(OUT_FILTERED_CSV_FP, index=False)