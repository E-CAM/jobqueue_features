FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Remove the previous conda installation and use our own, also installing OpenMPI and mpi4py
RUN rm -r /opt/anaconda
RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
RUN conda install --yes -c conda-forge python=3.8 dask distributed flake8 pytest pytest-asyncio openmpi mpi4py lammps
# Clone jobqueue and install it's requirements, and it
RUN git clone https://github.com/E-CAM/jobqueue_features.git
RUN pip install -r jobqueue_features/requirements.txt
# Add slurm configuration
COPY slurm.conf /etc/slurm/slurm.conf
# Use local setup as defaults
COPY jobqueue_features_slurm.yaml /jobqueue_features/jobqueue_features/jobqueue_features.yaml

