# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
role_node_and_edge.py {version}
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

def get_id(src, des, relation):
    '''
    生成规则：md5（起点id+终点id+关系类型）
    由于源ID有中文，因此这里需要做容错
    '''
    try:
        role_id = hashlib.md5(src + des + relation)
        return role_id.hexdigest()
    except:
        return ''

def is_invest(col):
    if 'INVEST' in col:
        return True
    else:
        return False

def get_role_label(label):
    return 'Entity;Role;{0}'.format(label.lower().capitalize())


def spark_data_flow():
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    get_role_label_udf = fun.udf(get_role_label, 
                                 tp.StringType())
    get_isinvest_label_udf = fun.udf(
        partial(get_role_label, 'Isinvest'), tp.StringType())
    get_id_udf = fun.udf(get_id, tp.StringType())
    get_isinvest_id_udf = fun.udf(
        partial(get_id, relation='Isinvest'), tp.StringType())
    is_invest_udf = fun.udf(is_invest, tp.BooleanType())
    
    get_relation_label_1_udf = fun.udf(
        partial(lambda r: r, 'IS'), tp.StringType())
    get_relation_label_2_udf = fun.udf(
        partial(lambda r: r, 'OF'), tp.StringType())
    get_relation_label_3_udf = fun.udf(
        partial(lambda r: r, 'VIRTUAL'), tp.StringType())    
    
    raw_role_df = spark.sql(
        '''
        SELECT 
        source_bbd_id           b,
        destination_bbd_id      c,
        relation_type           bc_relation
        FROM 
        dw.off_line_relations 
        WHERE 
        dt='{version}'  
        '''.format(version=RELATION_VERSION)
    ).dropDuplicates(
        ['b', 'c', 'bc_relation']
    ).cache()
    
    # Isinvest： 自定义的虚拟role节点
    tid_isinvest_role_df = raw_role_df.groupBy(
        ['b', 'c']
    ).agg(
        {'bc_relation': 'collect_set'}
    ).select(
        'b',
        get_isinvest_id_udf('b', 'c').alias('bbd_role_id:ID'),
        'c',
        is_invest_udf(
            'collect_set(bc_relation)'
        ).alias('relation_type:boolean')
    ).where(
        filter_comma_udf('b')
    ).where(
        filter_chinaese_udf('b')
    ).where(
        filter_comma_udf('c')
    ).where(
        filter_chinaese_udf('c')
    ).cache()
    
    prd_isinvest_role_node_df = tid_isinvest_role_df.where(
        tid_isinvest_role_df['bbd_role_id:ID'] != ''
    ).select(
        'bbd_role_id:ID',
        'relation_type:boolean',
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_isinvest_label_udf().alias(':LABEL')    
    )
    
    # role：角色节点
    tid_role_df = raw_role_df.select(
        'b',
        get_id_udf('b', 'c', 'bc_relation').alias('bbd_role_id:ID'),
        'c',
        'bc_relation'
    ).where(
        filter_comma_udf('b')
    ).where(
        filter_chinaese_udf('b')
    ).where(
        filter_comma_udf('c')
    ).where(
        filter_chinaese_udf('c')
    ).cache()
    
    prd_role_node_df = tid_role_df.where(
        tid_role_df['bbd_role_id:ID'] != ''
    ).select(
        'bbd_role_id:ID',
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),    
        get_role_label_udf('bc_relation').alias(':LABEL')
    )

    # Isinvest： 虚拟角色节点的关系
    prd_isinvest_role_edge_df = tid_isinvest_role_df.select(
        tid_isinvest_role_df.b.alias(':START_ID'), 
        tid_isinvest_role_df['bbd_role_id:ID'].alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_relation_label_3_udf().alias(':TYPE')
    ).union(
        tid_isinvest_role_df.select(
            tid_isinvest_role_df['bbd_role_id:ID'].alias(':START_ID'), 
            tid_isinvest_role_df.c.alias(':END_ID'),
            fun.unix_timestamp().alias('create_time:long'),
            get_relation_label_3_udf().alias(':TYPE')
        )
    )
    
    # role：角色节点的关系
    prd_role_edge_df = tid_role_df.select(
        tid_role_df.b.alias(':START_ID'), 
        tid_role_df['bbd_role_id:ID'].alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_relation_label_1_udf().alias(':TYPE')
    ).union(
        tid_role_df.select(
            tid_role_df['bbd_role_id:ID'].alias(':START_ID'), 
            tid_role_df.c.alias(':END_ID'),
            fun.unix_timestamp().alias('create_time:long'),
            get_relation_label_2_udf().alias(':TYPE')
        )
    )

    return (prd_isinvest_role_node_df, prd_role_node_df,
            prd_isinvest_role_edge_df, prd_role_edge_df)

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
    (prd_isinvest_role_node_df, prd_role_node_df,
     prd_isinvest_role_edge_df, prd_role_edge_df) = spark_data_flow()
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/isinvest_role_node
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_isinvest_role_node_df.write.csv(
        '{path}/{version}/isinvest_role_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/role_node
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_role_node_df.write.csv(
        '{path}/{version}/role_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))    

    os.system(
        '''
        hadoop fs -rmr {path}/{version}/isinvest_role_edge
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_isinvest_role_edge_df.write.csv(
        '{path}/{version}/isinvest_role_edge'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/role_edge
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_role_edge_df.where(
        prd_role_edge_df[':START_ID'] != ''
    ).where(
        prd_role_edge_df[':END_ID'] != ''
    ).write.csv(
        '{path}/{version}/role_edge'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
if __name__ == '__main__':
    # 输入参数
    RELATION_VERSION = '20170825'
    OUT_PATH = '/user/antifraud/source/tmp_test/tmp_file'
    
    run()
    
    