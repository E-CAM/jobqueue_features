#!/bin/bash

docker-compose up --build -d
while [ `docker exec -u pbsuser pbs-master pbsnodes -a | grep "Mom = pbs-slave" | wc -l` -ne 2 ]
do
    echo "Waiting for PBS slave nodes to become available";
    sleep 2
done
