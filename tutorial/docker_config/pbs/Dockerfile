# base image
FROM daskdev/dask-jobqueue:pbs
LABEL description="jobqueue_features customisation of dask_jobqueue CI images"

# Some customisation on jobqueue base images
ENV PBS_MASTER pbs-master
RUN yum install -y which && yum clean all

# Install MPICH and mpi4py (and black)
RUN conda install --yes -c conda-forge mpich mpi4py && conda clean -tipy

# Copy entrypoint and other needed scripts
COPY ./*master*.sh /
COPY ./*slave*.sh /
RUN chmod a+x ./*.sh

RUN git clone https://github.com/E-CAM/jobqueue_features.git

# default entrypoint launch pbs master
ENTRYPOINT ["bash", "/master-entrypoint.sh"]
