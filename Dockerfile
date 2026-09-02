FROM mcr.microsoft.com/devcontainers/python:3.13

SHELL ["/bin/bash", "-lc"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl bzip2 build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN arch="$(uname -m)" \
    && if [ "$arch" = "x86_64" ]; then miniforge_arch="x86_64"; \
    elif [ "$arch" = "aarch64" ] || [ "$arch" = "arm64" ]; then miniforge_arch="aarch64"; \
    else echo "Unsupported architecture: $arch" && exit 1; fi \
    && curl -fsSL "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-${miniforge_arch}.sh" -o /tmp/miniforge.sh \
    && bash /tmp/miniforge.sh -b -p /opt/conda \
    && rm -f /tmp/miniforge.sh

ENV PATH=/opt/conda/bin:/opt/conda/envs/movebench/bin:${PATH}

COPY environment.yml /tmp/environment.yml

RUN /opt/conda/bin/conda env create -f /tmp/environment.yml \
    && /opt/conda/bin/conda clean -afy
