#!/bin/bash


# start sge
sudo service gridengine-master restart

while ! ping -c1 slave-one &>/dev/null; do :; done

qconf -Msconf /scheduler.txt
qconf -Ahgrp /hosts.txt
qconf -Aq /queue.txt

qconf -ah slave-one
qconf -ah slave-two
qconf -ah slave-three

qconf -as $HOSTNAME
bash add_worker.sh dask.q slave-one 4
bash add_worker.sh dask.q slave-two 4

sudo service gridengine-master restart

sleep infinity
