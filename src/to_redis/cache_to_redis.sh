#!/bin/bash

/opt/spark-2.3.0/bin/spark-submit --master yarn --deploy-mode client --queue project.wanxiang /data8/wanxiang/zhaoyunfeng/Wanxiang/src/to_redis/cache_to_redis.py