# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
person_node.py {xgxx_relation} {relation_version}
'''
import sys
import os
import re

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp
from pyspark.sql.types import StructField, StructType, StringType


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

def get_label(x):
    try:
        return x.split(';')[2]
    except:
        return ''

def tmp_spark_data_flow():
    '''
    中间结果，后面“企业节点”计算也会用到
    '''
    role_node_struct = StructType([
        StructField("bbd_role_id", StringType(), True),
        StructField("role_name", StringType(), True),
        StructField("ratio", StringType(), True),
        StructField("create_time", StringType(), True),
        StructField("update_time", StringType(), True),
        StructField("LABEL", StringType(), True),
    ])
    
    role_edge_struct = StructType([
        StructField("START_ID", StringType(), True),
        StructField("END_ID", StringType(), True),
        StructField("create_time", StringType(), True),
        StructField("TYPE", StringType(), True)
    ])
    
    prd_role_edge_df = spark.read.csv(
        '{path}/{version}/role_edge'.format(
            path=IN_PATH,
            version=RELATION_VERSION),
        schema=role_edge_struct)
    
    prd_role_node_df = spark.read.csv(
        '{path}/{version}/role_node'.format(
            path=IN_PATH,
            version=RELATION_VERSION),
        schema=role_node_struct)
    
    tmp_role_df = prd_role_edge_df.join(
        prd_role_node_df,
        prd_role_edge_df['START_ID'] == prd_role_node_df['bbd_role_id'],
        'left_outer'
    ).select(
        prd_role_edge_df['START_ID'],
        prd_role_edge_df['END_ID'],
        prd_role_edge_df['TYPE'],
        prd_role_node_df.LABEL.alias('START_LABEL')
    ).join(
        prd_role_node_df,
        prd_role_edge_df['END_ID'] == prd_role_node_df['bbd_role_id'],
        'left_outer'
    ).select(
        prd_role_edge_df['START_ID'],
        prd_role_edge_df['END_ID'],
        'TYPE',
        'START_LABEL',
        prd_role_node_df.LABEL.alias('END_LABEL')
    )

    os.system(
        "hadoop fs -rmr {path}/tmp_role_df/{version}".format(path=TMP_PATH, 
                                              version=RELATION_VERSION))
    tmp_role_df.write.parquet(
        "{path}/tmp_role_df/{version}".format(path=TMP_PATH, 
                                              version=RELATION_VERSION))

def spark_data_flow():
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    get_person_label_udf = fun.udf(get_person_label, tp.StringType())
    is_human_udf = fun.udf(is_human, tp.BooleanType())
    get_label_udf = fun.udf(get_label , tp.StringType())

    raw_person_df = spark.sql(
        '''
        SELECT 
        source_bbd_id              b,
        destination_bbd_id         c,
        regexp_replace(source_name,'"','') b_name,
        regexp_replace(destination_name,'"','') c_name,
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
    
    # dwtzxx
    tmp_role_df = spark.read.parquet(
        "{path}/tmp_role_df/{version}".format(path=TMP_PATH, 
                                              version=RELATION_VERSION))
    
    tmp_dwtzxx_df = tmp_role_df.where(
        tmp_role_df.TYPE == 'IS'
    ).where(
        get_label_udf('END_LABEL') == 'Invest'
    ).groupBy(
        'START_ID'
    ).count(
    ).withColumnRenamed(
        'count', 'dwtzxx'
    ).withColumnRenamed(
        'START_ID', 'bbd_qyxx_id'
    ).cache()
    
    
    prd_person_df = tid_person_df.join(
        tmp_dwtzxx_df,
        tmp_dwtzxx_df.bbd_qyxx_id == tid_person_df.b,
        'left_outer'
    ).select(
        tid_person_df.b.alias('bbd_qyxx_id:ID'),
        tid_person_df.b_name.alias('name'),
        is_human_udf().alias('is_human:boolean'),
        tmp_dwtzxx_df.dwtzxx.alias('dwtzxx:int'),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_person_label_udf().alias(':LABEL')
    ).where(
        filter_chinaese_udf('bbd_qyxx_id:ID')
    ).where(
        filter_comma_udf('bbd_qyxx_id:ID')
    ).replace(
        '\\', ''
    ).replace(
        '"', ''
    ).dropDuplicates(
        ['bbd_qyxx_id:ID']
    ).fillna(
        0
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
    tmp_spark_data_flow()
    prd_person_df = spark_data_flow()

    # 输出
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/person_node
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_person_df.coalesce(600).write.csv(
        '{path}/{version}/person_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))    
    
    
if __name__ == '__main__':
    # 输入参数
    
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    TMP_PATH = '/user/wanxiang/tmpdata/'
    IN_PATH = '/user/wanxiang/step_one/'
    OUT_PATH = '/user/wanxiang/step_three/'

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    
    