# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
person_node.py {version}
'''

import os
import re

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp


def filter_comma(col):
    '''ID中逗号或为空值，则将该记录删除'''
    if not col or ',' in col or u'\uff0c' in col:
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

def is_human():
    return True

def get_person_label():
    return 'Entity;Person'

def spark_data_flow():
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    get_person_label_udf = fun.udf(get_person_label, tp.StringType())
    is_human_udf = fun.udf(is_human, tp.BooleanType())

    raw_person_df = spark.sql(
        '''
        SELECT 
        source_bbd_id              b,
        destination_bbd_id         c,
        source_name                b_name,
        destination_name           c_name,
        source_isperson            b_isperson,
        destination_isperson       c_isperson
        FROM 
        dw.off_line_relations 
        WHERE 
        dt='{version}'  
        '''.format(version=RELATION_VERSION)
    )
    
    tid_person_df = raw_person_df.select(
        'b', 'b_name', 'b_isperson'
    ).union(
        raw_person_df.select(
            'c', 'c_name', 'c_isperson'
        )
    ).where(
        'b_isperson == 1'
    ).dropDuplicates(
        ['b']
    ).cache()
    
    prd_person_df = tid_person_df.select(
        tid_person_df.b.alias('bbd_qyxx_id:ID'),
        tid_person_df.b_name.alias('name'),
        is_human_udf().alias('is_human:boolean'),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_person_label_udf().alias(':LABEL')
    ).where(
        filter_chinaese_udf('bbd_qyxx_id:ID')
    ).where(
        filter_comma_udf('bbd_qyxx_id:ID')
    )
    
    return prd_person_df

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "25g")
    conf.set("spark.executor.instances", 40)
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
        .appName("wanxiang_person_node") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    
def run():
    prd_person_df = spark_data_flow()

    # 输出
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/person_node
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_person_df.write.csv(
        '{path}/{version}/person_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))    
    
    
if __name__ == '__main__':
    # 输入参数
    RELATION_VERSION = '20170924'
    OUT_PATH = '/user/antifraud/source/tmp_test/tmp_file'

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    
    