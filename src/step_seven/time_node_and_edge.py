# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
time_node_and_edge.py {xgxx_relation} {relation_version}
'''

import sys
import os
import re
import datetime
import time
import calendar
import random
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

def get_time_label():
    return 'Entity;Time'

def re_date(date, return_type):
    try:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
        return date_obj.strftime(return_type)
    except:
        return 0

def get_month_range(year, month):
    days = calendar.monthrange(year, int(month))[1]
    return range(1, days+1)

def get_timestamp(date):
    '''将日期转换成linux时间戳'''
    try:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
        return long(time.mktime(date_obj.timetuple()))
    except:
        return 0

def  get_event_frequency_day(enent_time, time_range):
    try:
        return enent_time + '-' + str(random.randint(1, time_range-1))
    except:
        return None

def spark_data_flow():
    '''
    行业节点，可以根据“企业节点”的中间结果统计
    '''
    secondary_time_node_range = 101
    year_range = range(1970, 2025) 
    month_range = [
        '01', '02', '03',
        '04', '05', '06',
        '07', '08', '09',
        '10', '11', '12'
    ]
    
    get_time_label_udf = fun.udf(get_time_label, tp.StringType())
    get_time_relation_label_udf = fun.udf(
        partial(lambda r: r, 'BELONG'), tp.StringType())
    get_event_frequency_day_udf = fun.udf(
        partial(get_event_frequency_day, 
                time_range=secondary_time_node_range), tp.StringType())

    tid_xgxx_relation_df = spark.read.parquet(
        "{path}/tid_xgxx_relation_df/{version}".format(path=TMP_PATH, 
                                                       version=RELATION_VERSION)
    )
    

    # 年节点
    YEAR_LIST = [str(_) for _ in range(1970, 2025)]
    
    # 月节点
    MONTH_LIST = ['{0}-{1}'.format(year, month) 
                              for year in year_range
                              for month in month_range]
    # 日节点
    DAY_LIST = []
    FREQUENCY_LIST = []
    for year in year_range:
        for month in month_range:
            for day in get_month_range(year, month):
                if day < 10:
                    DAY_LIST.append('{0}-{1}-0{2}'.format(year, month, day))
                else:
                    DAY_LIST.append('{0}-{1}-{2}'.format(year, month, day))
                for frequency in range(1, secondary_time_node_range):
                    if day < 10:
                        FREQUENCY_LIST.append(
                            '{0}-{1}-0{2}-{3}'.format(year, month, 
                                                      day, frequency))
                    else:
                        FREQUENCY_LIST.append(
                            '{0}-{1}-{2}-{3}'.format(year, month, 
                                                     day, frequency))
                    
    raw_time_df = spark.sparkContext.parallelize(
        YEAR_LIST + MONTH_LIST + DAY_LIST + FREQUENCY_LIST
    ).map(
        lambda r: Row(time=r)
    ).toDF()

    prd_time_node_df = raw_time_df.select(
        raw_time_df.time.alias('time:ID'),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_time_label_udf().alias(':LABEL')
    )
    
    
    '''
    时间节点与其他节点的关系
    '''
        
    # 时间-时间关系
    YEAR_RELATION = [
        (YEAR_LIST[index-1], YEAR_LIST[index]) 
        for index in range(1, len(YEAR_LIST))]
    
    MONTH_RELATION = [
        ('{0}-{1}'.format(year, month), year) 
        for year in year_range
        for month in month_range]
    
    DAY_RELATION = []
    FREQUENCY_RELATION = []
    for year in year_range:
        for month in month_range:
            for day in get_month_range(year, month):
                if day < 10:
                    DAY_RELATION.append(
                        ('{0}-{1}-0{2}'.format(year, month, day), 
                         '{0}-{1}'.format(year, month)))
                else:
                    DAY_RELATION.append(
                        ('{0}-{1}-{2}'.format(year, month, day), 
                         '{0}-{1}'.format(year, month)))
                for frequency in range(1, secondary_time_node_range):
                    if day < 10:
                        FREQUENCY_RELATION.append(
                            ('{0}-{1}-0{2}-{3}'.format(year, month, 
                                                       day, frequency),
                             '{0}-{1}-0{2}'.format(year, month, day)))
                    else:
                        FREQUENCY_RELATION.append(
                            ('{0}-{1}-{2}-{3}'.format(year, month, 
                                                      day, frequency),
                             '{0}-{1}-{2}'.format(year, month, day)))
                    
    raw_time_edge_df = spark.sparkContext.parallelize(
        YEAR_RELATION + MONTH_RELATION + DAY_RELATION + FREQUENCY_RELATION
    ).map(
        lambda r: Row(src=r[0], des=r[1])
    ).toDF()
    
    prd_time_edge_1_df = raw_time_edge_df.select(
        raw_time_edge_df.src.alias(':START_ID'),
        raw_time_edge_df.des.alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_time_relation_label_udf().alias(':TYPE')
    )
    
    # 时间-事件关系
    tmp_time_edge_df = tid_xgxx_relation_df.select(
        'bbd_xgxx_id',
        get_event_frequency_day_udf('event_time').alias('event_time')
    ).dropDuplicates(
        ['bbd_xgxx_id']
    )
        
    prd_time_edge_2_df = tmp_time_edge_df.join(
        prd_time_node_df,
        prd_time_node_df['time:ID'] == tmp_time_edge_df.event_time
    ).select(
        tmp_time_edge_df.bbd_xgxx_id.alias(':START_ID'),
        tmp_time_edge_df.event_time.alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        get_time_relation_label_udf().alias(':TYPE')
    ).cache()        
        
    prd_time_edge_df = prd_time_edge_1_df.union(
        prd_time_edge_2_df    
    )
        
    return prd_time_node_df, prd_time_edge_df


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
        .appName("wanxiang_time_node_and_edge") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    
def run():
    prd_time_node_df, prd_time_edge_df = spark_data_flow()
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/time_node
        '''.format(path=OUT_PATH,
                   version=RELATION_VERSION))
    prd_time_node_df.coalesce(600).write.csv(
        '{path}/{version}/time_node'.format(path=OUT_PATH,
                                            version=RELATION_VERSION))    
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/time_edge
        '''.format(path=OUT_PATH,
                   version=RELATION_VERSION))
    prd_time_edge_df.coalesce(600).write.csv(
        '{path}/{version}/time_edge'.format(path=OUT_PATH,
                                            version=RELATION_VERSION))    
    
if __name__ == '__main__':
    # 输入参数
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    TMP_PATH = '/user/wanxiang/tmpdata/'
    OUT_PATH = '/user/wanxiang/step_seven/'
    
    #sparkSession
    spark = get_spark_session()
    
    run()
    
    