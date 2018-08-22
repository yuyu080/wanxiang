#!/bin/bash
# 在离线加载专用节点上无限循环跑，检查 /user/wanxiang/offline_signal 目录下有没有新文件夹生成
# 若有则表示可以进行新一轮 getmerge

new_version=`hadoop fs -ls hdfs://bbd43/user/wanxiang/offline_signal | awk '{print $8}' | sed '/^$/d'`
old_version=${new_version}
WORK_HOME='/data1/wanxiangneo4jpre'

while sleep 600
do
        new_version=`hadoop fs -ls hdfs://bbd43/user/wanxiang/offline_signal | awk '{print $8}' | sed '/^$/d'`
        for i in ${new_version}
        do
                flag=0
                for j in ${old_version}
                do
                        if [ ${i} = ${j} ]
                        then
                                flag=1
                        fi
                done
                if [ ${flag} -eq 0 ]
                then
                        echo "new version found"
                        version=`echo ${i} | grep -oP "(?<=/offline_signal/)(.*)"`
                        cd ${WORK_HOME}/Wanxiang/src/to_local
                        python to_local.py ${version} > \
                        to_local.log 2>&1
                        break
                fi
        done
        old_version=${new_version}
done