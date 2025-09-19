from typing import Dict, Any, Optional, Tuple, List
import os, math, yaml, json, requests
from shapely.geometry import box, mapping
import geopandas as gpd
import logging
from pystac_client import Client
import planetary_computer as pc  

logger = logging.getLogger(__name__)

##config loader
def load_settings() -> Tuple[dict, str]:
    here = os.path.dirname(os.path.abspath(__file__))             
    pipe_root = os.path.dirname(here)                              
    proj_root = os.path.dirname(pipe_root)                         
    candidates = [
        os.path.join(pipe_root, "config", "settings.yaml"),
        os.path.join(proj_root, "config", "settings.yaml"),
    ]
    for cfg in candidates:
        if os.path.exists(cfg):
            with open(cfg, "r", encoding="utf-8-sig") as f:
                return yaml.safe_load(f), cfg
    raise FileNotFoundError("settings.yaml not found in:\n  - " + "\n  - ".join(candidates))

config, _ = load_settings() 

AOI_BBOX = config["aoi"]["bbox"]
AOI_PATH = config["aoi"]["geojson_path"]
START_DATE = config["dates"]["start"]
END_DATE = config["dates"]["end"]
OUTPUT_DIR = config["download"]["output_dir"]
MAX_CLOUD_COVER = config["download"].get("max_cloud_cover", None)
MAX_ITEMS = config["download"].get("max_items", None)

STAC_ENDPOINT = config["stac"]["endpoint"]         
STAC_COLLECTION = config["stac"]["collection"]     

USE_INTERSECTS = bool(config.get("search", {}).get("use_intersects", True))

BBOX_PAD_KM = config.get("aoi", {}).get("bbox_pad_km", 0)

##helpers 
def pad_bbox_km(bbox, pad_km=0):
    if not pad_km or pad_km <= 0:
        return bbox
    minlon, minlat, maxlon, maxlat = bbox
    mean_lat = (minlat + maxlat) / 2.0
    dlat = pad_km / 111.0
    dlon = pad_km / (111.320 * max(0.01, math.cos(math.radians(mean_lat))))
    return (minlon - dlon, minlat - dlat, maxlon + dlon, maxlat + dlat)

def _is_geotiff_header(headers: Dict[str, str]) -> bool:
    ctype = (headers.get("Content-Type") or "").lower()
    return ("tiff" in ctype) or ("geotiff" in ctype) or ("image/tif" in ctype)

def _ensure_big_tif(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    size = os.path.getsize(path)
    if size < 1_000_000:
        raise ValueError(f"Downloaded file too small: {path} ({size} bytes)")

def _pick(assets: Dict[str, Any], names: List[str]) -> Optional[Any]:
    lower = {k.lower(): k for k in assets.keys()}
    for want in names:
        k = lower.get(want.lower())
        if k:
            return assets[k]
    return None

def _read_aoi_geom_wgs84(aoi_path: Optional[str], bbox: Optional[List[float]]):
    if aoi_path and os.path.exists(aoi_path):
        gdf = gpd.read_file(aoi_path)
        if gdf.crs is None:
            ##assuming WGS84 if missing
            gdf = gdf.set_crs("EPSG:4326")  
        gdf = gdf.to_crs(4326)
        geom = gdf.geometry.unary_union
        return mapping(geom)
    if bbox:
        minlon, minlat, maxlon, maxlat = bbox
        return mapping(box(minlon, minlat, maxlon, maxlat))
    return None

##download images
def download_landsat_scenes():
    ##build spatial filter
    aoi_geom = _read_aoi_geom_wgs84(AOI_PATH, AOI_BBOX)
    if aoi_geom is None:
        raise ValueError("No AOI provided. Set aoi.geojson_path or aoi.bbox in settings.yaml")

    ##search MPC STAC 
    logger.info(f"STAC query -> collection: {STAC_COLLECTION}, "
          f"dates: {START_DATE}/{END_DATE}, clouds <= {MAX_CLOUD_COVER if MAX_CLOUD_COVER is not None else 'ANY'}")
    cat = Client.open(STAC_ENDPOINT)

    query = {}
    if MAX_CLOUD_COVER is not None:
        query["eo:cloud_cover"] = {"lte": MAX_CLOUD_COVER}

    search_kwargs = dict(
        collections=[STAC_COLLECTION],
        datetime=f"{START_DATE}/{END_DATE}",
        query=query or None,
        limit=200
    )
    if USE_INTERSECTS:
        search_kwargs["intersects"] = aoi_geom
    else:
        ##fallback to bbox if intersects disabled
        minlon, minlat, maxlon, maxlat = AOI_BBOX or [19.0, 59.6, 31.6, 70.2]
        minlon, minlat, maxlon, maxlat = pad_bbox_km([minlon, minlat, maxlon, maxlat], BBOX_PAD_KM)
        search_kwargs["bbox"] = (minlon, minlat, maxlon, maxlat)

    search = cat.search(**search_kwargs)
    items = list(search.items())
    logger.info(f"Found {len(items)} STAC item(s)")

    if isinstance(MAX_ITEMS, int) and MAX_ITEMS > 0:
        items = items[:MAX_ITEMS]
        logger.warning(f"Limiting to {MAX_ITEMS} item(s) for test")

    if not items:
        print("0 items. Adjust dates/AOI/clouds.")
        return []

    ##debug first item
    a0 = items[0]
    logger.info(f"First item id: {a0.id}")
    logger.info(f"First item asset keys: {list((a0.assets or {}).keys())}")

    results = []
    scenes_with_bands, files_downloaded = 0, 0
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    ##sign + download per item
    for item in items:
        sitem = pc.sign(item)  ##add SAS tokens
        assets = sitem.assets or {}
        scene_id = sitem.id

        if scene_id.startswith("LE07"):
            logger.warning(f"Skipping Landsat 7 (SLC-off) scene: {scene_id}")
            continue

        ##prefer human-friendly keys; fall back to SR_B# variants
        a_red = _pick(assets, ["red", "SR_B3", "SR_B4", "B3", "B4", "B03", "B04"])
        a_nir = _pick(assets, ["nir08", "SR_B4", "SR_B5", "B4", "B5", "B04", "B05"])
        if not (a_red and a_nir):
            continue

        b4_out = os.path.join(OUTPUT_DIR, f"{scene_id}_SR_B4.TIF")
        b5_out = os.path.join(OUTPUT_DIR, f"{scene_id}_SR_B5.TIF")

        skip_scene = False
        for asset, dst in [(a_red, b4_out), (a_nir, b5_out)]:
            try:
                print(f"{scene_id}: {os.path.basename(dst)} <- {asset.href}")
                with requests.get(asset.href, stream=True, timeout=240) as r:
                    r.raise_for_status()
                    if not _is_geotiff_header(r.headers):
                        sample = (r.raw.read(1200) or b"").decode("utf-8", "ignore")
                        logger.warning(f"Non-TIFF response at: {asset.href}")
                        logger.info(f"Sample:\n{sample[:200]}")
                        skip_scene = True
                        break  
                    with open(dst, "wb") as f:
                        for chunk in r.iter_content(1024 * 1024):
                            if chunk:
                                f.write(chunk)
                _ensure_big_tif(dst)
                logger.info(f"{os.path.getsize(dst)/1e6:.1f} MB -> {dst}")
                files_downloaded += 1
            except Exception as e:
                logger.error(f"Failed to download {os.path.basename(dst)}: {e}")
                skip_scene = True
                break

        if skip_scene:
            logger.warning(f"Skipping scene: {scene_id}")
            continue

        results.append({
            "scene_id": scene_id,
            "B4": b4_out,
            "B5": b5_out
        })
        scenes_with_bands += 1

    logger.info(f"Summary: items={len(items)}, scenes_with_B4&B5={scenes_with_bands}, files_downloaded={files_downloaded}")
    logger.info(f"Prepared {len(results)} scene(s) with SR_B4 and SR_B5.")
    return results

if __name__ == "__main__":
    download_landsat_scenes()
