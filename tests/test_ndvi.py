import numpy as np
import rasterio
from src.transform.compute_ndvi import compute_ndvi
import tempfile
import os
##
def create_dummy_band(shape=(100, 100), value=1000, path=None):
    profile = {
        'driver': 'GTiff',
        'height': shape[0],
        'width': shape[1],
        'count': 1,
        'dtype': 'uint16',
        'crs': 'EPSG:4326',
        'transform': rasterio.transform.from_origin(0, 0, 0.1, 0.1),
        'nodata': 0
    }
    with rasterio.open(path, 'w', **profile) as dst:
        data = np.full(shape, value, dtype='uint16')
        dst.write(data, 1)

def test_compute_ndvi_basic():
    with tempfile.TemporaryDirectory() as tmpdir:
        b4_path = os.path.join(tmpdir, 'B4.tif')
        b5_path = os.path.join(tmpdir, 'B5.tif')
        out_path = os.path.join(tmpdir, 'ndvi.tif')

        create_dummy_band(value=1000, path=b4_path) 
        create_dummy_band(value=3000, path=b5_path)  

        compute_ndvi(b4_path, b5_path, out_path)

        with rasterio.open(out_path) as src:
            ndvi = src.read(1)
            assert np.all(np.isfinite(ndvi))
            assert ndvi.shape == (100, 100)
            assert np.all((ndvi >= -1.0) & (ndvi <= 1.0))
