#!/bin/bash
# 每分钟执行一次 check_index.py 脚本，检查索引是否全部建好

WORK_HOME='/home/wanxiangneo4jpre'

while sleep 60
do
        cd ${WORK_HOME}/Wanxiang/src/to_local
        python check_index.py $1
        status=$?
        if [[ ${status} -eq 0 ]];then
                break
        fi
done