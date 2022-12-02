# base image
FROM daskdev/dask-jobqueue:pbs
LABEL description="jobqueue_features customisation of dask_jobqueue CI images"

# Some customisation on jobqueue base images
ENV PBS_MASTER pbs-master
RUN yum install -y which && yum clean all

# Remove the previous conda installation and use our own, also installing OpenMPI and mpi4py
RUN rm -r /opt/anaconda
RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH

# Install required Python as well as MPICH, mpi4py and most of the requirements
ARG PYTHON_VERSION
ARG REQUIREMENTS
RUN conda install mamba -n base -c conda-forge
RUN REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt mamba install -c conda-forge --yes python=${PYTHON_VERSION} mpich-mpicc mpi4py black ${REQUIREMENTS}

# Copy entrypoint and other needed scripts
COPY ./*.sh /
RUN chmod a+x ./*.sh

# default entrypoint launch pbs master
ENTRYPOINT ["bash", "/master-entrypoint.sh"]
