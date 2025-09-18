FROM mambaorg/micromamba:1.5.8-bullseye

# Auto-activate the conda env in each RUN layer
ARG MAMBA_DOCKERFILE_ACTIVATE=1
SHELL ["/bin/bash", "-lc"]

# Work as mamba user and avoid permission issues
WORKDIR /app

# Copy env and create the env
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/env.yml
RUN micromamba create -y -n ndvi -f /tmp/env.yml && \
    micromamba clean --all -y

# Make GeoPandas prefer pyogrio so we don’t pull Fiona/GDAL wheels via pip
ENV GEOPANDAS_IO_ENGINE=pyogrio

# (Optional) put the env on PATH for entrypoint time
ENV PATH="/opt/conda/envs/ndvi/bin:${PATH}"

# Copy your project
COPY --chown=$MAMBA_USER:$MAMBA_USER . .

# Run your script (env is active due to ARG+SHELL above)
CMD ["python", "main.py"]
