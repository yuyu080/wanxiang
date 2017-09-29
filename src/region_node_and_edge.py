# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
region_node_and_edge.py {version}
'''

import os
import sys
import re
import datetime
import time
import json
from functools import partial
import hashlib

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp,


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

def get_region_label():
    return 'Entity;Region'

def spark_data_flow():
    get_region_label_udf = fun.udf(get_region_label, tp.StringType())
    get_region_relation_label_udf = fun.udf(
        partial(lambda r: r, 'BELONG'), tp.StringType())
    
    # 城市中间数据
    prd_basic_df = spark.read.parquet(
        '''
        hadoop fs -rmr {path}/prd_basic_df/{version}
        '''.format(path=TMP_PATH, 
                   version=RELATION_VERSION)
    )
    
    # 地域映射表
    mapping_df = spark.read.csv(
        '{path}/{file_name}'.format(path=TMP_PATH, 
                                    file_name=FILE_NAME),
        sep='\t',
        header=True
    ).withColumnRenamed(
        'code', 'company_county'
    ).dropDuplicates(
        ['company_county']
    )    
    
    raw_region_df = mapping_df.where(
        mapping_df.county.isNotNull()
    ).select(
        'county', 'company_county'
    ).union(
        mapping_df.where(
            mapping_df.county.isNull()
        ).where(
            mapping_df.city.isNotNull()
        ).select(
            'city', 'company_county'
        )
    ).union(
        mapping_df.where(
            mapping_df.county.isNull()
        ).where(
            mapping_df.city.isNull()
        ).where(
            mapping_df.province.isNotNull()
        ).select(
            'province', 'company_county'
        )
    ).withColumnRenamed(
        'county', 'region'
    ).cache()
    
    # 地域公司数量分布
    tmp_region_df = prd_basic_df.select(
        'company_county'
    ).groupBy(
        'company_county'
    ).count(
    ).withColumnRenamed(
        'count', 'company_num'
    )
    
    # 输出
    prd_region_node_df = raw_region_df.join(
        tmp_region_df,
        'company_county',
        'left_outer'
    ).select(
        raw_region_df.company_county.alias('region_code:ID'),
        fun.when(
            tmp_region_df.company_num.isNotNull(), 
            tmp_region_df.company_num
        ).otherwise(
            0
        ).alias('company_num:int'),
        raw_region_df.region.alias('name'),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_region_label_udf().alias(':LABEL')
    ).cache()

    '''
    地域节点与其他节点的关系
    '''

    # 企-区业关系
    prd_region_edge_1_df = prd_basic_df.join(
        mapping_df.where(
            mapping_df.county.isNotNull()
        ).select(
            'county', 'company_county'
        ),
        'company_county'
    ).select(
        prd_basic_df.bbd_qyxx_id.alias(':START_ID'), 
        prd_basic_df.company_county.alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_region_relation_label_udf().alias(':TYPE')
    )
    
    # 区-市关系
    prd_region_edge_2_df = mapping_df.select(
        'city',
        'county',
        'company_county'
    ).where(
        mapping_df.county.isNotNull()
    ).join(
        prd_region_df,
        prd_region_df.name == mapping_df.city
    ).select(
        mapping_df.company_county.alias(':START_ID'),
        prd_region_df['region_code:ID'].alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_region_relation_label_udf().alias(':TYPE')
    )
    
    # 市-省关系
    prd_region_edge_3_df = mapping_df.select(
        'province',
        'city',
        'county',
        'company_county'
    ).where(
        mapping_df.county.isNull()
    ).where(
        mapping_df.city.isNotNull()
    ).where(
        mapping_df.province.isNotNull()
    ).join(
        prd_region_df,
        prd_region_df.name == mapping_df.province
    ).select(
        mapping_df.company_county.alias(':START_ID'),
        prd_region_df['region_code:ID'].alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_region_relation_label_udf().alias(':TYPE')
    )
    
    # 事件-区域关系
    # 由于这个关系不好解析，并且玩法太多，这里暂时不管
    # 后面如果要拿来讲故事，那么可以手工构建该类关系
    prd_region_edge_4_df = ''


    prd_region_edge_df = prd_region_edge_1_df.union(
        prd_region_edge_2_df
    ).union(
        prd_region_edge_3_df
    )

    return prd_region_node_df, prd_region_edge_df

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
    prd_region_node_df, prd_region_edge_df = spark_data_flow()

    os.system(
        '''
        hadoop fs -rmr {path}/{version}/region_node
        '''.format(path=OUT_PATH,
                   version=RELATION_VERSION))
    prd_region_df.write.csv(
        '{path}/{version}/region_node'.format(path=OUT_PATH, 
                                              version=RELATION_VERSION))

    os.system(
        '''
        hadoop fs -rmr {path}/{version}/region_edge
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
    prd_region_edge_df.write.csv(
        '{path}/{version}/region_edge'.format(path=OUT_PATH,
                                              version=RELATION_VERSION))

    
if __name__ == '__main__':
    # 输入参数
    RELATION_VERSION = '20170825'
    FILE_NAME = 'company_county_mapping_20170524.data'
    TMP_PATH = '/user/antifraud/graph_relation_construction'
    OUT_PATH = '/user/antifraud/source/tmp_test/tmp_file'
    
    run()
    
    