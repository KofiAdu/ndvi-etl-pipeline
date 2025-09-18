import os
import json
import yaml
from rasterio._env import get_gdal_data
from shapely.geometry import box, mapping  
from src.extract.download_landsat_stac import download_landsat_scenes
from src.transform.compute_ndvi import compute_ndvi, clip_raster_to_aoi
from src.load.load_to_postgis import run_loader

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
    print(f"PROJ set to: {proj_dir}")
except Exception as e:
    print(f"Could not set PROJ paths: {e}")

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
        print(f"Created AOI GeoJSON at {aoi_abs}")
    else:
        print(f"Using existing AOI GeoJSON at {aoi_abs}")

    return aoi_abs


def run_pipeline():
    ##load config
    config, CONFIG_PATH = load_settings()
    print(f"Using config: {CONFIG_PATH}")

    ##check if AOI file exists
    AOI_PATH = ensure_aoi_geojson_from_bbox(config['aoi']['bbox'], config['aoi']['geojson_path'])

    here = os.path.dirname(os.path.abspath(__file__))
    PROCESSED_DIR = os.path.join(here, "data", "processed")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    scenes = download_landsat_scenes()

    if not scenes:
        print("No scenes downloaded.")
        return

    success_count = 0
    for s in scenes:
        try:
            scene_id = s['scene_id']
            b4_path = s['B4']
            b5_path = s['B5']

            ndvi_output = os.path.join(PROCESSED_DIR, f"{scene_id}_NDVI.tif")
            clipped_output = os.path.join(PROCESSED_DIR, f"{scene_id}_NDVI_clipped.tif")

            print(f"Computing NDVI for {scene_id} ...")
            compute_ndvi(b4_path, b5_path, ndvi_output)

            print(f"Clipping NDVI to AOI for {scene_id} ...")
            clip_raster_to_aoi(ndvi_output, AOI_PATH, clipped_output)

            print(f"Done: {clipped_output}")
            success_count += 1

        except Exception as e:
            print(f"Failed on {scene_id}: {e}")

    print(f"Pipeline complete. Successful scenes: {success_count}/{len(scenes)}")
    print("Loading processed results into PostGIS...")

    ##load to postgis db
    run_loader() 


if __name__ == "__main__":
    import sys, traceback
    print(f">>> Python: {sys.executable}", flush=True)
    print(f">>> Entry: {__file__}", flush=True)
    try:
        print(">>> Starting run_pipeline()", flush=True)
        run_pipeline()
        print(">>> Finished run_pipeline()", flush=True)
    except SystemExit as se:
        print(f"!!! SystemExit: {se}", flush=True)
        raise
    except Exception:
        print("!!! Unhandled exception:", flush=True)
        traceback.print_exc()
        raise