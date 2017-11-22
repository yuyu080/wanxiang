# -*- coding: utf-8 -*-
'''
根据输入的日期参数，提前该日期前的所有黑名单企业

提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
balck_list_to_redis.py {xgxx_relation}
'''

import sys
import datetime

import redis
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp


def filter_date(date):
    try:
        date_time = datetime.datetime.strptime(XGXX_RELATION, '%Y-%m-%d').date()
        if date < date_time:
            return True
        else:
            return False
    except:
        return False


def spark_data_flow():
    '''
    利用spark从mysql中读取黑名单数据
    '''
    filter_date_udf = fun.udf(filter_date, tp.BooleanType())
    
    black = spark.read.jdbc(url=URL, table="black_list", 
                            properties=PROP)
    
    black_qyxx_id = black.where(
        filter_date_udf('create_time')
    ).select(
        'bbd_qyxx_id'
    ).distinct(
    ).rdd.map(
        lambda r: r.bbd_qyxx_id
    ).collect()
    
    return black_qyxx_id
    

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "25g")
    conf.set("spark.executor.instances", 10)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)

    spark = SparkSession \
        .builder \
        .appName("wanxiang_time_node_and_edge") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 


def run():
    black_qyxx_id = spark_data_flow()
    pool = redis.ConnectionPool(host='10.28.60.15', port=26382, 
                                password='wanxiang', db=0)
    r = redis.Redis(connection_pool=pool)
    
    pipe = r.pipeline(transaction=True)
    
    for each_id in black_qyxx_id:
        r.sadd('wx_graph_black_set', each_id)
    
    pipe.execute()
    
    print "SUCESS !!"
    
if __name__ == '__main__':
    # 输入参数
    XGXX_RELATION = sys.argv[1]
    URL='jdbc:mysql://mysql12.prod.bbdops.com:53606/bbd_higgs?characterEncoding=UTF-8'
    PROP = {"user": "airpal", 
            "password":"G2sorqM82RcVoPrb8z5V", 
            "driver": "com.mysql.jdbc.Driver",
            "ip": "mysql12.prod.bbdops.com",
            "db_name": "bbd_higgs",
            "port": "53606"}
    
    #sparkSession
    spark = get_spark_session()
    
    run()