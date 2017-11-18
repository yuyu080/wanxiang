# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
real_off_line_relations.py {relation_version}
'''
import sys
import os
import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp


def analysis(col):
    '''
    json解析
    '''
    try:
        analysised_dict_list = json.loads(col)
        return analysised_dict_list
    except:
        return None

def add_qyxx_id(qyxx_id, qyxx_bgxx):
    '''
    将id加到bgxx的每一条数据中
    '''
    try:
        if len(qyxx_bgxx) > 0:
            return [dict(each_info, 
                         **{'bbd_qyxx_id' : qyxx_id}) 
                    for each_info in qyxx_bgxx]
        else:
            return None
    except:
        return None
    
def get_id():
    '''
    获取一个空字段
    '''
    return None
    
def repalce_null_value(col):
    '''
    将''替换成'NULL'
    '''
    try:
        return col if col != '' else None
    except:
        return None

def spark_data_flow():
    string_to_list_udf = fun.udf(
        analysis, 
        tp.ArrayType(tp.MapType(tp.StringType(), tp.StringType())))
    add_qyxx_id_udf = fun.udf(
        add_qyxx_id, 
        tp.ArrayType(tp.MapType(tp.StringType(), tp.StringType())))
    
    def get_basic_df():
        '''
        获取【法人】关联方
        '''
        #将条数展开
        basic_df = raw_df.select(
            fun.explode(
                add_qyxx_id_udf(
                    'bbd_qyxx_id', 
                    string_to_list_udf('qyxx_basic')
                )
            ).alias('qyxx_basic')
        )
        basic_df.select(    
            basic_df.qyxx_basic.getItem('bbd_qyxx_id').alias('bbd_qyxx_id'),
            basic_df.qyxx_basic.getItem('company_name').alias('company_name'),
            basic_df.qyxx_basic.getItem('frname').alias('frname'),
            basic_df.qyxx_basic.getItem('frname_id').alias('frname_id'),
            basic_df.qyxx_basic.getItem('frname_compid').alias('frname_compid')
        ).createOrReplaceTempView('basic')
        
        os.system(
            '''
            hadoop fs -rmr {path}/{version}/basic_df
            '''.format(path=TMP_PATH,
                       version=RELATION_VERSION))
        
        spark.sql(
            '''
            SELECT 
            '' company_name,
            '' bbd_qyxx_id,
            frname source_name,
            frname_id source_bbd_id,
            0 source_degree,
            frname_compid source_isperson,
            company_name destination_name,
            bbd_qyxx_id destination_bbd_id,
            0 destination_degree,
            0 destination_isperson,
            UPPER('legal') relation_type,
            '' position
            FROM
            basic
            '''
        ).write.parquet(
            '{path}/{version}/basic_df'.format(path=TMP_PATH,
                                               version=RELATION_VERSION))
    
    def get_baxx_df():
        '''
        获取【董监高】关联方
        '''
        #将条数展开
        baxx_df = raw_df.select(
            fun.explode(
                add_qyxx_id_udf(
                    'bbd_qyxx_id', 
                    string_to_list_udf('qyxx_baxx')
                )
            ).alias('qyxx_baxx')
        )
        
        baxx_df.select(
            baxx_df.qyxx_baxx.getItem('bbd_qyxx_id').alias('bbd_qyxx_id'),
            baxx_df.qyxx_baxx.getItem('company_name').alias('company_name'),
            baxx_df.qyxx_baxx.getItem('name').alias('name'),
            baxx_df.qyxx_baxx.getItem('name_id').alias('name_id'),
            baxx_df.qyxx_baxx.getItem('type').alias('type'),
            baxx_df.qyxx_baxx.getItem('position').alias('position'),
        ).createOrReplaceTempView('baxx')
        
        
        import os
        
        os.system(
            '''
            hadoop fs -rmr {path}/{version}/baxx_df
            '''.format(path=TMP_PATH,
                       version=RELATION_VERSION))
        
        
        spark.sql(
            '''
            SELECT 
            '' company_name,
            '' bbd_qyxx_id,
            name source_name,
            name_id source_bbd_id,
            0 source_degree,
            1 source_isperson,
            company_name destination_name,
            bbd_qyxx_id destination_bbd_id,
            0 destination_degree,
            0 destination_isperson,
            UPPER(type) relation_type,
            position position
            FROM
            baxx
            '''
        ).distinct(
        ).write.parquet(
            '{path}/{version}/baxx_df'.format(path=TMP_PATH,
                                              version=RELATION_VERSION))
    
    def get_gdxx_df():
        '''
        获取【股东】关联方
        '''
        #将条数展开
        gdxx_df = raw_df.select(
            fun.explode(
                add_qyxx_id_udf(
                    'bbd_qyxx_id', 
                    string_to_list_udf('qyxx_gdxx')
                )
            ).alias('qyxx_gdxx')
        )
        
        gdxx_df.select(
            gdxx_df.qyxx_gdxx.getItem('bbd_qyxx_id').alias('bbd_qyxx_id'),
            gdxx_df.qyxx_gdxx.getItem('company_name').alias('company_name'),
            gdxx_df.qyxx_gdxx.getItem('shareholder_id').alias('shareholder_id'),
            gdxx_df.qyxx_gdxx.getItem('shareholder_name').alias('shareholder_name'),
            gdxx_df.qyxx_gdxx.getItem('shareholder_type').alias('shareholder_type'),
            gdxx_df.qyxx_gdxx.getItem('name_compid').alias('name_compid'),
        ).createOrReplaceTempView('gdxx')
        
        os.system(
            '''
            hadoop fs -rmr {path}/{version}/gdxx_df
            '''.format(path=TMP_PATH,
                       version=RELATION_VERSION))
        
        spark.sql(
            '''
            SELECT 
            '' company_name,
            '' bbd_qyxx_id,
            shareholder_name source_name,
            shareholder_id source_bbd_id,
            0 source_degree,
            name_compid source_isperson,
            company_name destination_name,
            bbd_qyxx_id destination_bbd_id,
            0 destination_degree,
            0 destination_isperson,
            UPPER('invest') relation_type,
            shareholder_type position
            FROM
            gdxx
            '''
        ).distinct(
        ).write.parquet(
            '{path}/{version}/gdxx_df'.format(path=TMP_PATH,
                                              version=RELATION_VERSION))

    # 触发计算逻辑
    raw_df = spark.read.json(
        '{path}/{file}'.format(path=IN_PATH,
                               version=RELATION_VERSION)).cache()    
    get_basic_df()
    get_baxx_df()
    get_gdxx_df()
                   
    # 数据落地
    basic_df = spark.read.parquet(
        '{path}/{version}/basic_df'.format(path=TMP_PATH,
                                          version=RELATION_VERSION))
    gdxx_df = spark.read.parquet(
        '{path}/{version}/gdxx_df'.format(path=TMP_PATH,
                                          version=RELATION_VERSION))
    baxx_df = spark.read.parquet(
        '{path}/{version}/baxx_df'.format(path=TMP_PATH,
                                          version=RELATION_VERSION))
    
    off_line_relations = basic_df.union(
        gdxx_df
    ).union(
        baxx_df
    ).distinct()
    
    off_line_relations.createOrReplaceTempView('off_line_relations')
    
    spark.sql(
        '''
        insert 
        overwrite table wanxiang.off_line_relations partition (dt={version})
        select * from off_line_relations
        '''.format(version=RELATION_VERSION)
    )
        
def run():
    spark_data_flow()
    

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "25g")
    conf.set("spark.executor.instances", 50)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.python.worker.memory", "3g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)

    spark = SparkSession \
        .builder \
        .appName("wanxiang_real_off_line_relations") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    
if __name__ == '__main__':
    # 输入参数
    RELATION_VERSION = sys.argv[2]
    
    FILE_NAME = 'BUSINESS_REAL_TIME_BBD_HIGGS_QYXX_20171117'
    IN_PATH = '/user/antifraud/source/tmp_test/tmp_file/'
    TMP_PATH = '/user/wanxiang/tmpdata/'
    OUT_PATH = '/user/wanxiang/step_two/'

    #sparkSession
    spark = get_spark_session()
    
    run()