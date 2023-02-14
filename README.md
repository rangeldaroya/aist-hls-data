# Setup Instructions
1. Activate conda environment (Note this repo uses Python 3.8.13)
2. Install requirements using `pip install -r requirements.txt`
3. Go to `src` directory: `cd src`
3. Run code (NOTE: make sure to update the global variables to reflect correct filepaths):
    - `python 00_filter_csv.py`
    - `python 01_download_hls.py`

# Structure of Files
<OUT_DIR>
    |--<site_id>
        |--<date>
            |--<band1.tiff>
            |--<band2.tiff>
            .
            .
            |--<bandn.tiff>