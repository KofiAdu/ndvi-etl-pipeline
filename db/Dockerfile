FROM postgis/postgis:15-3.3

RUN apt-get update && apt-get install -y --no-install-recommends \
      gdal-bin \
      libgdal28 \
    && rm -rf /var/lib/apt/lists/*


ENV GDAL_DATA=/usr/share/gdal/3.2
