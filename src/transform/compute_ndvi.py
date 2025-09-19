import numpy as np
import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.transform import array_bounds
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.warp import transform_geom
from shapely.geometry import box, shape
from shapely.errors import TopologicalError
import yaml, os, logging

logger = logging.getLogger(__name__)

WRITE_LOCAL_CLIP = os.getenv("WRITE_LOCAL_CLIP", "0") == "1"
WRITE_LOCAL_VIZ  = os.getenv("WRITE_LOCAL_VIZ",  "0") == "1"

##config reader for product options
def _load_product_opts():
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates = [
        os.path.join(here, "config", "settings.yaml"),
        os.path.join(os.path.dirname(here), "config", "settings.yaml"),
    ]
    for p in candidates:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8-sig") as f:
                cfg = yaml.safe_load(f)
            prod = cfg.get("products", {}) if cfg else {}
            return prod.get("reproject_crs", None), bool(prod.get("build_overviews", False))
    return None, False

def compute_ndvi(b4_path, b5_path, out_path):
    ##landsat c2 l2 scaling (surface reflectance)
    scale, offset = 0.0000275, -0.2

    with rasterio.open(b4_path) as r4, rasterio.open(b5_path) as r5:
        if (r4.width, r4.height, r4.transform) != (r5.width, r5.height, r5.transform):
            raise ValueError("B4 and B5 rasters are not on the same grid.")

        red = r4.read(1).astype("float32") * scale + offset
        nir = r5.read(1).astype("float32") * scale + offset

        invalid = ~np.isfinite(red) | ~np.isfinite(nir)

        with np.errstate(divide='ignore', invalid='ignore'):
            ndvi = (nir - red) / (nir + red)
        ndvi[invalid] = np.nan

        profile = r4.profile.copy()
        profile.update(driver="GTiff", dtype="float32", count=1, nodata=-9999.0,
                       compress="deflate", predictor=3, zlevel=6)

        ndvi_out = np.where(np.isfinite(ndvi), ndvi, -9999.0).astype("float32")
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(ndvi_out, 1)

    logger.info(f"NDVI saved to {out_path}")
    return out_path

def clip_raster_to_aoi(raster_path, aoi_path, out_path):
    with rasterio.open(raster_path) as src:
        dst_crs = src.crs
        ras_bounds = array_bounds(src.height, src.width, src.transform)
        ras_poly_dst = box(*ras_bounds)

        ##show raster footprint in WGS84 for sanity check
        try:
            ras_poly_wgs = shape(transform_geom(dst_crs, "EPSG:4326", ras_poly_dst.__geo_interface__))
            logger.info(f"Raster bounds (WGS84): {tuple(round(v, 4) for v in ras_poly_wgs.bounds)}")
        except Exception:
            pass

        aoi = gpd.read_file(aoi_path)
        if aoi.empty:
            raise ValueError(f"AOI is empty: {aoi_path}")
        if aoi.crs is None:
            aoi = aoi.set_crs("EPSG:4326")
        logger.info(f"AOI bounds (WGS84): {tuple(round(v, 4) for v in aoi.to_crs(4326).total_bounds)}")

        try:
            aoi_proj = aoi.to_crs(dst_crs)
        except Exception as e:
            raise ValueError(f"Failed to reproject AOI to raster CRS {dst_crs}: {e}")

        try:
            geom = aoi_proj.geometry.unary_union
            if geom.is_empty:
                raise ValueError("AOI geometry became empty after reprojection.")
            geom = geom.buffer(0)
        except TopologicalError:
            geom = aoi_proj.buffer(0).unary_union

        if not geom.intersects(ras_poly_dst):
            ##small buffer in raster units (in this case meters for UTM)
            if not geom.buffer(1.0).intersects(ras_poly_dst):
                raise ValueError("Input shapes do not overlap raster.")
            geom = geom.buffer(1.0)

        out_arr, out_transform = mask(src, shapes=[geom.__geo_interface__], crop=True, nodata=src.nodata)
        out_meta = src.meta.copy()
        out_meta.update({"height": out_arr.shape[1], "width": out_arr.shape[2], "transform": out_transform})

    with rasterio.open(out_path, "w", **out_meta) as dst:
        dst.write(out_arr)

    logger.info(f"Clipped raster saved to {out_path}")

    ##build overviews and/or reproject
    target_crs, build_ovr = _load_product_opts()
    if build_ovr:
        with rasterio.open(out_path, "r+") as ds:
            ds.build_overviews([2, 4, 8, 16, 32], Resampling.average)
            ds.update_tags(ns="rio_overview", resampling="average")
        logger.info("Built internal overviews")

    if target_crs:
        reproj_path = os.path.splitext(out_path)[0] + "_viz.tif"
        _reproject_raster(out_path, reproj_path, target_crs)
        if build_ovr:
            with rasterio.open(reproj_path, "r+") as ds:
                ds.build_overviews([2, 4, 8, 16, 32], Resampling.average)
                ds.update_tags(ns="rio_overview", resampling="average")
        logger.info(f"Reprojected for viz -> {target_crs}: {reproj_path}")

    return out_path

def _reproject_raster(in_path, out_path, target_crs):
    with rasterio.open(in_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({"crs": target_crs, "transform": transform, "width": width, "height": height})
        with rasterio.open(out_path, "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.bilinear,  ##continuous NDVI
                )
