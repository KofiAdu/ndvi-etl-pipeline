from pathlib import Path
from datetime import datetime
import time
import os
import math
import psycopg2
import logging
from psycopg2 import OperationalError
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import geopandas as gpd
import numpy as np
from tempfile import NamedTemporaryFile

logger = logging.getLogger(__name__)
DEFAULT_TARGET_EPSG = int(os.getenv("DEFAULT_TARGET_EPSG", "32635"))

def _utm_epsg_for_lonlat(lon: float, lat: float) -> int:
    zone = int(math.floor((lon + 180.0) / 6.0) + 1)
    return (32600 if lat >= 0 else 32700) + zone  

def choose_target_epsg(geojson_path: Path) -> int:
    """
    If AOI has a projected CRS (meters), use it.
    If AOI is geographic (e.g., 4326) or missing, compute correct UTM from centroid.
    Fallback to DEFAULT_TARGET_EPSG if needed.
    """
    gdf = gpd.read_file(geojson_path)
    if gdf.crs is not None:
        epsg = gdf.crs.to_epsg()
        if epsg and epsg not in (4326, 4258): 
            return int(epsg)
    geom = gdf.geometry.unary_union
    c = geom.centroid
    try:
        return _utm_epsg_for_lonlat(float(c.x), float(c.y))
    except Exception:
        return DEFAULT_TARGET_EPSG


def _connect_with_retry():
    host = os.getenv("POSTGRES_HOST", "localhost")  
    db   = os.getenv("POSTGRES_DB", "Crop_Health")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd  = os.getenv("POSTGRES_PASSWORD", "12345")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    for attempt in range(1, 31):
        try:
            return psycopg2.connect(
                dbname=db, user=user, password=pwd, host=host, port=port, connect_timeout=3
            )
        except OperationalError as e:
            logger.error(f"DB not ready (try {attempt}/30): {e}")
            time.sleep(2)
    raise RuntimeError("Database never became reachable.")

##helpers
def _epsg_from_file_or_none(tif_path: Path):
    """Return EPSG int if present in file, else None (do NOT invent)."""
    try:
        with rasterio.open(tif_path) as src:
            if src.crs:
                auth = src.crs.to_authority()
                if auth and auth[0] == "EPSG":
                    return int(auth[1])
                e = src.crs.to_epsg()
                if e:
                    return int(e)
    except Exception:
        pass
    return None

def _nanmean(band, nodata):
    arr = band.astype("float32", copy=False)
    if nodata is not None:
        arr = np.where(arr == nodata, np.nan, arr)
    arr = np.where(np.isfinite(arr), arr, np.nan)
    return None if np.isnan(arr).all() else float(np.nanmean(arr))

def safe_execute(cursor, sql, params):
    try:
        cursor.execute(sql, params)
        return True
    except Exception as e:
        print(f"Insert failed: {e}")
        cursor.connection.rollback()
        return False

def _reproject_to_epsg(src_path: Path, target_epsg: int, res_m: float = 30.0) -> Path:
    """
    Reproject src_path to target_epsg at a fixed meter resolution.
    Returns original path if already in target, else a temp file path.
    """
    with rasterio.open(src_path) as src:
        if src.crs is None:
            raise ValueError(f"{src_path.name} has no CRS; cannot reproject safely.")
        src_epsg = src.crs.to_epsg()
        if src_epsg == target_epsg:
            return src_path

        dst_crs = f"EPSG:{target_epsg}"
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds,
            dst_resolution=(res_m, res_m)
        )
        meta = src.meta.copy()
        meta.update({
            "crs": dst_crs,
            "transform": transform,
            "width": width,
            "height": height,
            "driver": "GTiff",
            "tiled": True,
            "compress": meta.get("compress", "deflate"),
            "BIGTIFF": "IF_SAFER",
        })
        nodata = meta.get("nodata", src.nodata)

        with NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        with rasterio.open(tmp_path, "w", **meta) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear if src.dtypes[i-1].startswith("float") else Resampling.nearest,
                    src_nodata=nodata,
                    dst_nodata=nodata,
                )
        return tmp_path

##AOIs
def get_aoi_id(cursor, aoi_name: str = "AOI") -> int:
    cursor.execute("SELECT id FROM aois WHERE name=%s", (aoi_name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("SELECT id FROM aois ORDER BY id LIMIT 1")
    row = cursor.fetchone()
    if row:
        print(f"AOI named '{aoi_name}' not found. Using first available AOI (id={row[0]}).")
        return row[0]
    raise RuntimeError("No AOIs found in database. You must load AOIs first.")

def load_aois(cursor, geojson_path: Path):
    print("\nLoading AOIs...")
    gdf = gpd.read_file(geojson_path)
    gdf = gdf.set_crs(4326) if gdf.crs is None else gdf.to_crs(4326)

    count = 0
    for idx, row in gdf.iterrows():
        name = row.get("name", f"aoi_{idx}")
        geom_wkt = row.geometry.wkt
        ok = safe_execute(cursor, """
            INSERT INTO aois (name, geom)
            VALUES (%s, ST_Multi(ST_GeomFromText(%s, 4326)))
            ON CONFLICT (name) DO NOTHING;
        """, (name, geom_wkt))
        if ok:
            count += 1
    logger.info(f"Loaded {count} AOI(s)")


##rasters
def load_ndvi_full(cursor, ndvi_dir: Path, target_epsg: int):
    logger.info("\nLoading full-scene NDVI rasters...")
    for tif_path in ndvi_dir.glob("*_NDVI.tif"):
        if "clipped" in tif_path.name:
            continue
        logger.info(f"  -> {tif_path.name}")

        parts = tif_path.stem.split('_')
        try:
            scene_id = '_'.join(parts[0:7])
            date_str = parts[3]
            acquisition_date = datetime.strptime(date_str, "%Y%m%d").date()
            sensor = parts[0]
        except Exception as e:
            logger.warning(f"Skipping invalid filename: {tif_path.name} | {e}")
            continue

        try:
            with rasterio.open(tif_path) as src:
                if src.width == 0 or src.height == 0:
                    logger.warning(f"Skipping raster with 0 width/height: {tif_path.name}")
                    continue
        except Exception as e:
            logger.error(f"Could not open {tif_path.name} as raster: {e}")
            continue

        try:
            reproj_path = _reproject_to_epsg(tif_path, target_epsg, res_m=30.0)
        except Exception as e:
            logger.warning(f"Skipping {tif_path.name}: reprojection failed: {e}")
            continue

        with open(reproj_path, "rb") as f:
            raster_data = f.read()

        sql = """
            INSERT INTO ndvi_full (scene_id, acquisition_date, sensor, cloud_cover, raster)
            VALUES (%s, %s, %s, %s, ST_SetSRID(ST_FromGDALRaster(%s), %s))
            ON CONFLICT (scene_id) DO NOTHING;
        """
        params = (scene_id, acquisition_date, sensor, None, raster_data, target_epsg)
        safe_execute(cursor, sql, params)

        if reproj_path != tif_path:
            try: os.remove(reproj_path)
            except Exception: pass

    logger.info("Full NDVI loaded.")

def load_ndvi_clipped(cursor, ndvi_dir: Path, aoi_id: int, target_epsg: int):
    logger.info("\nLoading clipped NDVI rasters...")
    for tif_path in ndvi_dir.glob("*_NDVI_clipped.tif"):
        if "viz" in tif_path.name:
            continue
        print(f"  → {tif_path.name}")

        parts = tif_path.stem.split('_')
        try:
            scene_id = '_'.join(parts[0:7])
            date_str = parts[3]
            acquisition_date = datetime.strptime(date_str, "%Y%m%d").date()
        except Exception as e:
            logger.warning(f"Skipping invalid filename: {tif_path.name} | {e}")
            continue

        cursor.execute("SELECT id FROM ndvi_full WHERE scene_id = %s", (scene_id,))
        r = cursor.fetchone()
        if not r:
            logger.warning(f"Skipping {tif_path.name}, full NDVI not found")
            continue
        full_id = r[0]

        try:
            with rasterio.open(tif_path) as src:
                band = src.read(1)
                mean_ndvi = _nanmean(band, src.nodata)
        except Exception as e:
            logger.error(f"Could not read {tif_path.name} to compute mean: {e}")
            continue

        try:
            reproj_path = _reproject_to_epsg(tif_path, target_epsg, res_m=30.0)
        except Exception as e:
            logger.info(f"Skipping {tif_path.name}: reprojection failed: {e}")
            continue

        with open(reproj_path, "rb") as f:
            raster_data = f.read()

        sql = """
            INSERT INTO ndvi_clipped (full_id, aoi_id, acquisition_date, mean_ndvi, raster)
            VALUES (%s, %s, %s, %s, ST_SetSRID(ST_FromGDALRaster(%s), %s))
            ON CONFLICT (full_id, aoi_id) DO UPDATE
              SET acquisition_date = EXCLUDED.acquisition_date,
                  mean_ndvi        = EXCLUDED.mean_ndvi,
                  raster           = EXCLUDED.raster;
        """
        params = (full_id, aoi_id, acquisition_date, mean_ndvi, raster_data, target_epsg)
        safe_execute(cursor, sql, params)

        if reproj_path != tif_path:
            try: os.remove(reproj_path)
            except Exception: pass

    logger.info("Clipped NDVI loaded.")

def load_ndvi_viz(cursor, ndvi_dir: Path, aoi_id: int):
    logger.info("\nLoading NDVI viz rasters...")
    for tif_path in ndvi_dir.glob("*_NDVI_clipped_viz.tif"):
        parts = tif_path.stem.split('_')
        try:
            scene_id = '_'.join(parts[0:7])
            date_str = parts[3]
            acquisition_date = datetime.strptime(date_str, "%Y%m%d").date()
        except Exception as e:
            logger.warning(f"Skipping invalid filename: {tif_path.name} | {e}")
            continue

        cursor.execute("SELECT id FROM ndvi_full WHERE scene_id=%s", (scene_id,))
        r = cursor.fetchone()
        if not r:
            logger.warning(f"No full NDVI for {scene_id}; skipping viz.")
            continue
        full_id = r[0]

        cursor.execute("SELECT id FROM ndvi_clipped WHERE full_id=%s AND aoi_id=%s", (full_id, aoi_id))
        r2 = cursor.fetchone()
        if not r2:
            logger.warning(f"No clipped NDVI for {scene_id} / AOI {aoi_id}; skipping viz.")
            continue
        clipped_id = r2[0]

        style = "default"
        try:
            reproj_path = _reproject_to_epsg(tif_path, 3857, res_m=30.0)
        except Exception as e:
            logger.warning(f"Skipping viz {tif_path.name}: reprojection failed: {e}")
            continue

        with open(reproj_path, "rb") as f:
            raster_data = f.read()

        sql = """
            INSERT INTO ndvi_viz (clipped_id, aoi_id, acquisition_date, style, raster)
            VALUES (%s, %s, %s, %s, ST_SetSRID(ST_FromGDALRaster(%s), %s))
            ON CONFLICT (clipped_id) DO UPDATE
              SET acquisition_date = EXCLUDED.acquisition_date,
                  style            = EXCLUDED.style,
                  raster           = EXCLUDED.raster;
        """
        params = (clipped_id, aoi_id, acquisition_date, style, raster_data, 3857)
        safe_execute(cursor, sql, params)

        if reproj_path != tif_path:
            try: os.remove(reproj_path)
            except Exception: pass

    logger.info("NDVI viz loaded.")


##raster constraints for QGIS needs srid in raster_columns
def drop_raster_constraints(cursor):
    logger.warning("\nDropping raster constraints (if present) on ndvi_* tables...")
    for tbl in ("ndvi_full", "ndvi_clipped", "ndvi_viz"):
        safe_execute(
            cursor,
            "SELECT DropRasterConstraints('public'::name, %s::name, 'raster'::name);",
            (tbl,),
        )
    cursor.connection.commit()
    print("Done dropping raster constraints.")

def add_raster_constraints_metadata(cursor):
    logger.info("Adding raster metadata constraints for QGIS...")
    for tbl in ("ndvi_full", "ndvi_clipped", "ndvi_viz"):
        ok = safe_execute(
            cursor,
            "SELECT AddRasterConstraints('public'::name, %s::name, 'raster'::name);",
            (tbl,),
        )
        if not ok:
            logger.warning(f"Warning: failed to add constraints for {tbl}")
    cursor.connection.commit()
    logger.info("Added metadata constraints.")


def run_loader():
    logger.info("\nStarting full ETL Load to PostGIS...")
    ndvi_dir = Path("data/processed")
    geojson_path = Path("data/aoi/boundary.geojson")

    conn = _connect_with_retry()
    cursor = conn.cursor()
    try:
        target_epsg = choose_target_epsg(geojson_path)
        logger.info(f"Using TARGET_EPSG={target_epsg} for rasters (AOI-driven)")

        drop_raster_constraints(cursor)

        load_aois(cursor, geojson_path)
        conn.commit()

        aoi_id = get_aoi_id(cursor, "AOI")

        load_ndvi_full(cursor, ndvi_dir, target_epsg)
        conn.commit()

        load_ndvi_clipped(cursor, ndvi_dir, aoi_id, target_epsg)
        conn.commit()

        load_ndvi_viz(cursor, ndvi_dir, aoi_id)  
        conn.commit()

        add_raster_constraints_metadata(cursor)

    finally:
        cursor.close()
        conn.close()

    logger.info("\nETL Load complete.")
