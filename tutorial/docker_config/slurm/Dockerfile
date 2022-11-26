FROM daskdev/dask-jobqueue:slurm

# Some customisation on jobqueue base images
# Remove the previous conda installation and use our own, also installing OpenMPI and mpi4py
RUN rm -r /opt/anaconda
RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
ARG PYTHON_VERSION
ARG REQUIREMENTS
RUN REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt conda install --yes python=${PYTHON_VERSION}
RUN conda install mamba -n base -c conda-forge
RUN mamba install --yes -c conda-forge openmpi-mpicc mpi4py lammps
RUN mamba install --yes -c conda-forge black ${REQUIREMENTS} && conda clean -tipy
# Clone jobqueue and install it's requirements, and it
RUN git clone https://github.com/E-CAM/jobqueue_features.git 
RUN cd /jobqueue_features; pip install --upgrade dask_jobqueue dask distributed; pip install --no-deps -e .
# Add slurm configuration
COPY slurm.conf /etc/slurm/slurm.conf
# Use local setup as defaults
COPY jobqueue_features_slurm.yaml /jobqueue_features/jobqueue_features/jobqueue_features.yaml

