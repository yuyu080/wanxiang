#!/bin/bash

while sleep 60
do
        python ./check_index.py $1
        status=$?
        if [[ ${status} -eq 0 ]];then
                break
        fi
done