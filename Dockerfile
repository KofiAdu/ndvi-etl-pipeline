FROM mambaorg/micromamba:1.5.8-bullseye

ARG MAMBA_DOCKERFILE_ACTIVATE=1
SHELL ["/bin/bash", "-lc"]

WORKDIR /app

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/env.yml
RUN micromamba create -y -n ndvi -f /tmp/env.yml && \
    micromamba clean --all -y

ENV GEOPANDAS_IO_ENGINE=pyogrio

ENV PATH="/opt/conda/envs/ndvi/bin:${PATH}"

COPY --chown=$MAMBA_USER:$MAMBA_USER . .

CMD ["python", "main.py"]
