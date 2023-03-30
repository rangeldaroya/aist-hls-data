''' Runs inference on a given GeoTIFF image.
Code below is from https://github.com/isikdogan/deepwatermap
'''
import deepwatermap
import numpy as np
import cv2
import rasterio
import os

TO_SAVE_MASKS = True   # Set to True if masks should be saved for visual checks
OUT_DIR = f"../outputs/"  # where to save thresholded water mask 0/1 only (only if TO_SAVE_MASKS=True)
MODEL_CKPT_PATH = "../checkpoints/cp.135.ckpt" # path to trained model

def find_padding(v, divisor=32):
    v_divisible = max(divisor, int(divisor * np.ceil( v / divisor )))
    total_pad = v_divisible - v
    pad_1 = total_pad // 2
    pad_2 = total_pad - pad_1
    return pad_1, pad_2

def get_water_mask(model_path, image, out_dir, MASK_THRESH=0.5):
    """
    Params:
        - model_path: path to the trained model checkpoint
        - image: multispectral image composed of bands 2 to 7 from HLS (size: 6,h,w)
        - out_dir: where to save image mask if masks are to be saved
    Returns: water mask array (1 for water pixels, np.nan for non-water pixels)
    """
    # load the model
    model = deepwatermap.model()
    model.load_weights(model_path).expect_partial()    # see https://github.com/tensorflow/tensorflow/issues/43554

    image = np.transpose(image, (1,2,0))
    pad_r = find_padding(image.shape[0])
    pad_c = find_padding(image.shape[1])
    image = np.pad(image, ((pad_r[0], pad_r[1]), (pad_c[0], pad_c[1]), (0, 0)), 'reflect')

    # solve no-pad index issue after inference
    if pad_r[1] == 0:
        pad_r = (pad_r[0], 1)
    if pad_c[1] == 0:
        pad_c = (pad_c[0], 1)

    image = image.astype(np.float32)

    # remove nans (and infinity) - replace with 0s
    image = np.nan_to_num(image, copy=False, nan=0.0, posinf=0.0, neginf=0.0)
    
    image = image - np.min(image)
    image = image / np.maximum(np.max(image), 1)

    # run inference
    image = np.expand_dims(image, axis=0)
    dwm = model.predict(image)
    dwm = np.squeeze(dwm)
    dwm = dwm[pad_r[0]:-pad_r[1], pad_c[0]:-pad_c[1]]

    # soft threshold
    dwm = 1./(1+np.exp(-(16*(dwm-0.5))))
    dwm = np.clip(dwm, 0, 1)
    
    water_mask = np.where(dwm > MASK_THRESH, 1, np.nan)
    if TO_SAVE_MASKS:
        thresh_dwm = np.where(dwm > MASK_THRESH, 255, 0)
        cv2.imwrite(os.path.join(out_dir, "water_mask_out.png"), thresh_dwm)

    return water_mask

if __name__ == '__main__':
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)
    
    # Sample use of the code. This assumes there's a `../bands/*.tif` HLS tiles
    bands = ["../bands/HLS.L30.T18STH.2015229T154610.v2.0."]
    for ctr, image_path in enumerate(bands):
        # load and preprocess the input image (might need to change this depending on the file format)
        # only need bands 2 to 7 for HLS dataset
        if image_path.endswith("."):
            B02 = rasterio.open(f'{image_path}B02.tif').read()
            B03 = rasterio.open(f'{image_path}B03.tif').read()
            B04 = rasterio.open(f'{image_path}B04.tif').read()
            B05 = rasterio.open(f'{image_path}B05.tif').read()
            B06 = rasterio.open(f'{image_path}B06.tif').read()
            B07 = rasterio.open(f'{image_path}B07.tif').read()
        else:
            B02 = rasterio.open(f'{image_path}B2.TIF').read()
            B03 = rasterio.open(f'{image_path}B3.TIF').read()
            B04 = rasterio.open(f'{image_path}B4.TIF').read()
            B05 = rasterio.open(f'{image_path}B5.TIF').read()
            B06 = rasterio.open(f'{image_path}B6.TIF').read()
            B07 = rasterio.open(f'{image_path}B7.TIF').read()
        image = np.concatenate([B02, B03, B04, B05, B06, B07], axis=0)  # size: (6, h, w)
        water_mask = get_water_mask(model_path=MODEL_CKPT_PATH, image=image, out_dir=OUT_DIR)
        print(water_mask)
