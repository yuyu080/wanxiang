#!/bin/bash
# 每分钟检查一次，看 HDFS 文件系统中某路径下是否有新的文件夹生成，如果有则说明索引全部建好，那么给开发人员发微信通知

SCKEY=SCU18362T36dadf900509742623c554ff37500c765a37802f84f04

today=`date +%Y-%m-%d`
version=$1
path='/tmp/success_'${version}

while sleep 60
do
        hadoop fs -test -e ${path}

        if [ $? -eq 0 ] ;then
                curl --request GET --url "https://sc.ftqq.com/${SCKEY}.send?text=索引创建完成";
                break
        fi
done