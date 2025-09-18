-- Use Polygon because your AOI is a single polygon (keeps it simple).
CREATE TABLE IF NOT EXISTS aois (
  id   SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  geom geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_aois_geom ON aois USING GIST (geom);

CREATE TABLE IF NOT EXISTS ndvi_full (
  id               SERIAL PRIMARY KEY,
  scene_id         TEXT UNIQUE,
  acquisition_date DATE,
  sensor           TEXT,
  cloud_cover      FLOAT,
  raster           raster
);

CREATE TABLE IF NOT EXISTS ndvi_clipped (
  id               SERIAL PRIMARY KEY,
  full_id          INTEGER REFERENCES ndvi_full(id) ON DELETE CASCADE,
  aoi_id           INTEGER REFERENCES aois(id) ON DELETE CASCADE,
  acquisition_date DATE,
  mean_ndvi        FLOAT,
  raster           raster,
  UNIQUE (full_id, aoi_id)
);

CREATE TABLE IF NOT EXISTS ndvi_viz (
  id               SERIAL PRIMARY KEY,
  clipped_id       INTEGER UNIQUE REFERENCES ndvi_clipped(id) ON DELETE CASCADE,
  aoi_id           INTEGER REFERENCES aois(id) ON DELETE CASCADE,
  acquisition_date DATE,
  style            TEXT,
  raster           raster
);
