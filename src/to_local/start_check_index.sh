#!/bin/bash

while sleep 60
do
        cd /data1/wanxiangneo4jpre/Wanxiang/src/to_local
        python check_index.py $1
        status=$?
        if [[ ${status} -eq 0 ]];then
                break
        fi
done