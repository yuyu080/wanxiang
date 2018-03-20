# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
contact_node_and_edge.py {xgxx_relation} {relation_version} 
'''
import sys
import os
import re

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp

def filter_comma(col):
    '''ID中逗号或为空值，则将该记录删除'''
    if not col or ',' in col or u'\uff0c' in col or '"' in col or '\\' in col:
        return False
    else:
        return True

def filter_chinaese(col):
    '''字段中只要包含中文，将其过滤'''
    if col:
        match = re.search(ur'[\u4e00-\u9fa5]', col)
        return False if match else True
    else:
        return False

def spark_data_flow():
    get_phone_label_udf = fun.udf(
        lambda x: 'Entity;Contact;Phone', tp.StringType())
    get_email_label_udf = fun.udf(
        lambda x: 'Entity;Contact;Email', tp.StringType())
    get_phone_type_udf = fun.udf(
        lambda x: 'PHONE', tp.StringType())
    get_email_type_udf = fun.udf(
        lambda x: 'EMAIL', tp.StringType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    
    raw_nb_df = spark.sql(
        """
        SELECT 
        bbd_qyxx_id,
        phone,
        email,
        year
        FROM
        dw.qyxx_annual_report_jbxx 
        WHERE
        dt='{version}'
        """.format(version=XGXX_RELATION)
    )
    
    tid_nb_df = raw_nb_df.where(
        "bbd_qyxx_id != 'null'"
    ).where(
        "phone != 'null'"
    ).where(
        "email != 'null'"
    ).where(
        raw_nb_df.bbd_qyxx_id.isNotNull()
    ).where(
        raw_nb_df.phone.isNotNull()
    ).where(
        raw_nb_df.email.isNotNull()
    ).where(
        filter_comma_udf('bbd_qyxx_id')
    ).where(
        filter_comma_udf('phone')
    ).where(
        filter_comma_udf('email')
    ).cache()
    
    prd_phone_node_df = tid_nb_df.select(
        tid_nb_df.phone.alias('bbd_contact_id:ID'),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_phone_label_udf('phone').alias(':LABEL')
    ).distinct()
    
    prd_phone_edge_df = tid_nb_df.select(
        tid_nb_df.bbd_qyxx_id.alias(':START_ID'),
        tid_nb_df.phone.alias(':END_ID'),
        tid_nb_df.year.alias('year'),
        fun.unix_timestamp().alias('create_time:long'),
        get_phone_type_udf('phone').alias(':TYPE')
    ).distinct()
    
    prd_email_node_df =  tid_nb_df.select(
        tid_nb_df.email.alias('bbd_contact_id:ID'),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_email_label_udf('email').alias(':LABEL')
    ).distinct()
    
    prd_email_edge_df = tid_nb_df.select(
        tid_nb_df.bbd_qyxx_id.alias(':START_ID'),
        tid_nb_df.email.alias(':END_ID'),
        tid_nb_df.year.alias('year'),
        fun.unix_timestamp().alias('create_time:long'),
        get_email_type_udf('email').alias(':TYPE')
    ).distinct()

    return (prd_phone_node_df,prd_phone_edge_df,
            prd_email_node_df,prd_email_edge_df)

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "25g")
    conf.set("spark.executor.instances", 30)
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
        .appName("wanxiang_event_node_and_edge") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark
    
def run():    
    (prd_phone_node_df,prd_phone_edge_df,
     prd_email_node_df,prd_email_edge_df) = spark_data_flow()
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/*
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
    prd_phone_node_df.write.csv(
        '{path}/{version}/phone_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
    prd_phone_edge_df.write.csv(
        '{path}/{version}/phone_edge'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))    
    
    prd_email_node_df.write.csv(
        '{path}/{version}/email_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))

    prd_email_edge_df.write.csv(
        '{path}/{version}/email_edge'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))    

if __name__ == '__main__':
    # 输入参数
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    OUT_PATH = '/user/wanxiang/step_eight/'

    #sparkSession
    spark = get_spark_session()
    
    run()
    