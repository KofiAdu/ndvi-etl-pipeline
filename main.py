import logging
import os
import sys, traceback
import json
import yaml
import time
from rasterio._env import get_gdal_data
from time import perf_counter
from datetime import datetime
from shapely.geometry import box, mapping  
from src.extract.download_landsat_stac import download_landsat_scenes
from src.transform.compute_ndvi import compute_ndvi, clip_raster_to_aoi
from src.load.load_to_postgis import run_loader

##log directory
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

##timestamp log file
log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

##logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


try:
    from pyproj import datadir
    os.environ.pop("PROJ_LIB", None)
    os.environ.pop("PROJ_DATA", None)
    proj_dir = datadir.get_data_dir()
    os.environ["PROJ_LIB"] = proj_dir
    os.environ["PROJ_DATA"] = proj_dir
    try:  
        gdal_data = get_gdal_data()
        if gdal_data:
            os.environ["GDAL_DATA"] = gdal_data
    except Exception:
        pass
    os.environ.setdefault("PROJ_NETWORK", "ON")
    logger.info(f"PROJ set to: {proj_dir}")
except Exception as e:
    logger.error(f"Could not set PROJ paths: {e}")

##get settings 
def load_settings():
    here = os.path.dirname(os.path.abspath(__file__))         
    root = os.path.dirname(here)                                
    candidates = [
        os.path.join(here, "config", "settings.yaml"),         
        os.path.join(root, "config", "settings.yaml"),          
    ]
    for cfg in candidates:
        if os.path.exists(cfg):
            with open(cfg, "r", encoding="utf-8-sig") as f:
                return yaml.safe_load(f), cfg
    raise FileNotFoundError("settings.yaml not found in:\n  - " + "\n  - ".join(candidates))


def ensure_aoi_geojson_from_bbox(bbox, aoi_path):
    """Create a GeoJSON bbox polygon at aoi_path if it doesn't exist. Returns absolute path."""
    here = os.path.dirname(os.path.abspath(__file__))
    aoi_abs = aoi_path if os.path.isabs(aoi_path) else os.path.join(here, aoi_path)
    os.makedirs(os.path.dirname(aoi_abs), exist_ok=True)

    if not os.path.exists(aoi_abs):
        minlon, minlat, maxlon, maxlat = bbox
        geom = box(minlon, minlat, maxlon, maxlat)
        fc = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": mapping(geom),
                "properties": {"name": "AOI", "crs": "EPSG:4326"}
            }]
        }
        with open(aoi_abs, "w", encoding="utf-8") as f:
            json.dump(fc, f)
        logger.info(f"Created AOI GeoJSON at {aoi_abs}")
    else:
        logger.info(f"Using existing AOI GeoJSON at {aoi_abs}")

    return aoi_abs


def run_pipeline():
    ##load config
    config, CONFIG_PATH = load_settings()
    logger.info(f"Using config: {CONFIG_PATH}")

    ##check if AOI file exists
    AOI_PATH = ensure_aoi_geojson_from_bbox(config['aoi']['bbox'], config['aoi']['geojson_path'])

    here = os.path.dirname(os.path.abspath(__file__))
    PROCESSED_DIR = os.path.join(here, "data", "processed")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    scenes = download_landsat_scenes()

    if not scenes:
        print("No scenes downloaded.")
        return

    start = perf_counter()

    success_count = 0
    failure_count = 0
    failures = []

    for s in scenes:
        scene_id = s.get("scene_id", "unknown")
        try:
            b4_path = s['B4']
            b5_path = s['B5']

            ndvi_output = os.path.join(PROCESSED_DIR, f"{scene_id}_NDVI.tif")
            clipped_output = os.path.join(PROCESSED_DIR, f"{scene_id}_NDVI_clipped.tif")

            logger.info(f"Computing NDVI for {scene_id} ...")
            compute_ndvi(b4_path, b5_path, ndvi_output)

            logger.info(f"Clipping NDVI to AOI for {scene_id} ...")
            clip_raster_to_aoi(ndvi_output, AOI_PATH, clipped_output)

            logger.info(f"Done: {clipped_output}")
            success_count += 1

        except Exception as e:
            logger.error(f"Failed on {scene_id}: {e}")
            failures.append((scene_id, str(e)))
            failure_count += 1

    duration = perf_counter() - start

    logger.info("\nPipeline Summary:")
    logger.info(f"  - Total scenes    : {len(scenes)}")
    logger.info(f"  - Successful      : {success_count}")
    logger.info(f"  - Failed          : {failure_count}")
    logger.info(f"  - Duration        : {duration:.2f} seconds")

    if failures:
        logger.info("Failure details:")
        for sid, reason in failures:
            logger.info(f"    - {sid}: {reason}")

    logger.info(f"Pipeline complete. Successful scenes: {success_count}/{len(scenes)}")
    logger.info("Loading processed results into PostGIS...")

    ##load to postgis db
    run_loader() 


if __name__ == "__main__":
    logger.info(f">>> Python: {sys.executable}")
    logger.info(f">>> Entry: {__file__}")
    try:
        logger.info(">>> Starting run_pipeline()")
        start_time = time.time()
        run_pipeline()
        duration = time.time() - start_time
        logger.info(">>> Finished run_pipeline()")
    except SystemExit as se:
        logger.info(f"!!! SystemExit: {se}")
        raise
    except Exception:
        logger.info("!!! Unhandled exception:")
        traceback.print_exc()
        raise