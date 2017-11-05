# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
industry_node_and_edge.py {xgxx_relation} {relation_version}
'''

import sys
import os
import re
from functools import partial

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp, Row


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

def get_industry_label():
    return 'Entity;Industry'

def spark_data_flow():
    '''
    行业节点，可以根据“企业节点”的中间结果统计
    '''
    get_industry_label_udf = fun.udf(get_industry_label, tp.StringType())
    get_industry_relation_label_udf = fun.udf(
        partial(lambda r: r, 'BELONG'), tp.StringType())
    
    prd_basic_df = spark.read.parquet(
        "{path}/prd_basic_df/{version}".format(path=TMP_PATH, 
                                               version=RELATION_VERSION)
    )

    industry_data = [ 
        ('A', u'农、林、牧、渔服务业'),
        ('B', u'采矿业'),
        ('C', u'制造业'),
        ('D', u'电力、热力、燃气及水生产和供应业'),
        ('E', u'建筑业'),
        ('F', u'批发和零售业'),
        ('G', u'交通运输、仓储和邮政业'),
        ('H', u'住宿和餐饮业'),
        ('I', u'信息传输、软件和信息技术服务业'),
        ('J', u'金融业'),
        ('K', u'房地产业'),
        ('L', u'租赁和商务服务业'),
        ('M', u'科学研究和技术服务业'),
        ('N', u'水利、环境和公共设施管理业'),
        ('O', u'居民服务、修理和其他服务业'),
        ('P', u'教育'),
        ('Q', u'卫生和社会工作'),
        ('R', u'文化、体育和娱乐业'),
        ('S', u'公共管理、社会保障和社会组织'),
        ('T', u'国际组织'),
        ('Z', u'其他'),
    ]
    
    raw_industry_df = spark.sparkContext.parallelize(
        industry_data
    ).map(
        lambda r: Row(name=r[1], company_industry=r[0])
    ).toDF()
    
    tmp_industry_df = prd_basic_df.select(
        'company_industry'
    ).groupBy(
        'company_industry'
    ).count(
    ).withColumnRenamed(
        'count', 'company_num'
    )
    
    prd_industry_node_df = raw_industry_df.join(
        tmp_industry_df,
        'company_industry',
        'left_outer'
    ).select(
        raw_industry_df.company_industry.alias('industry_code:ID'),
        raw_industry_df.name,
        fun.when(
            tmp_industry_df.company_num.isNotNull(), 
            tmp_industry_df.company_num
        ).otherwise(
            0
        ).alias('company_num:int'),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_industry_label_udf().alias(':LABEL')
    )

    '''
    行业节点与其他节点的关系
    '''
    prd_industry_edge_df = prd_basic_df.join(
        raw_industry_df,
        'company_industry'
    ).select(
        prd_basic_df.bbd_qyxx_id.alias(':START_ID'),
        prd_basic_df.company_industry.alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_industry_relation_label_udf().alias(':TYPE')
    )    
    
    return prd_industry_node_df, prd_industry_edge_df

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
        .appName("wanxiang_industry_node_and_edge") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    
def run():
    prd_industry_node_df, prd_industry_edge_df = spark_data_flow()

    os.system(
        '''
        hadoop fs -rmr {path}/{version}/industry_node
        '''.format(path=OUT_PATH,
                   version=RELATION_VERSION))
    prd_industry_node_df.coalesce(600).write.csv(
        '{path}/{version}/industry_node'.format(path=OUT_PATH,
                                                version=RELATION_VERSION))

    os.system(
        '''
        hadoop fs -rmr {path}/{version}/industry_edge
        '''.format(path=OUT_PATH,
                   version=RELATION_VERSION))
    prd_industry_edge_df.coalesce(600).write.csv(
        '{path}/{version}/industry_edge'.format(path=OUT_PATH,
                                                version=RELATION_VERSION))
    
if __name__ == '__main__':
    # 输入参数
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    IN_PATH = '/user/wanxiang/inputdata'
    TMP_PATH = '/user/wanxiang/tmpdata'
    OUT_PATH = '/user/wanxiang/step_one'

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    