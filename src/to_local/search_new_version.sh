#!/bin/bash
# 在离线加载专用节点上无限循环跑，检查 /user/wanxiang/offline_signal 目录下有没有新文件夹生成
# 若有则表示可以进行新一轮 getmerge

new_version=`hadoop fs -ls hdfs://bbd43/user/wanxiang/offline_signal | awk '{print $8}' | sed '/^$/d'`
old_version=${new_version}

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
                        echo 'find new version'
                        version=`echo ${i} | grep -oP "(?<=/offline_signal/)(.*)"`
                        echo 'waiting for new version to be merged'
                        sleep 20
                        python /data1/wanxiangneo4jpre/Wanxiang/src/to_local/to_local.py ${version}
                        echo 'merged complete'
                        break
                fi
        done
        old_version=${new_version}
done