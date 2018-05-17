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

def filter_bad_case(relation_type, des_id, source_isperson):
    '''
    过滤把企业当做自然人的错误节点: 
    假如一个节点在baxx表中，同时该节点的标签本为Company，那么就将这个关系过滤
    '''
    if (relation_type != 'INVEST' and 
            relation_type != 'LEGAL' and 
            des_id and source_isperson):
        return False
    else:
        return True

def spark_data_flow():
    string_to_list_udf = fun.udf(
        analysis, 
        tp.ArrayType(tp.MapType(tp.StringType(), tp.StringType())))
    filt = fun.udf(filter_bad_case, tp.BooleanType())
    
    def get_basic_df():
        '''
        获取【法人】关联方
        '''
        #将条数展开
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
            dw.qyxx_basic
            WHERE
            dt='{version}'
            '''.format(version=RELATION_VERSION)
        ).write.parquet(
            '{path}/{version}/basic_df'.format(path=TMP_PATH,
                                               version=RELATION_VERSION))
    
    def get_baxx_df():
        '''
        获取【董监高】关联方
        '''
        #将条数展开     
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
            name_compid source_isperson,
            company_name destination_name,
            bbd_qyxx_id destination_bbd_id,
            0 destination_degree,
            0 destination_isperson,
            UPPER(type) relation_type,
            position position
            FROM
            dw.qyxx_baxx
            WHERE
            dt='{version}'
            '''.format(version=RELATION_VERSION)
        ).distinct(
        ).write.parquet(
            '{path}/{version}/baxx_df'.format(path=TMP_PATH,
                                              version=RELATION_VERSION))
    
    def get_gdxx_df():
        '''
        获取【股东】关联方
        '''
        #将条数展开
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
            dw.qyxx_gdxx
            WHERE
            dt='{version}'
            '''.format(version=RELATION_VERSION)
        ).distinct(
        ).write.parquet(
            '{path}/{version}/gdxx_df'.format(path=TMP_PATH,
                                              version=RELATION_VERSION))

    # 触发计算逻辑
    raw_yisi = spark.sql(
        '''
        SELECT
        qyxx_id qyxxId,
        group_id newGroupId,
        person_name personName
        FROM
        dw.uniq_person_id_v2
        WHERE
        dt='{version}'
        '''.format(version=RELATION_VERSION)
    )
    
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

    # 剔除source中的企业节点
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/tmp_off_line_companys
        '''.format(path=TMP_PATH,
                   version=RELATION_VERSION))
    off_line_relations.select(
        off_line_relations.destination_bbd_id.alias('tmp_company_name')
    ).distinct().write.parquet(
        '{path}/{version}/tmp_off_line_companys'.format(path=TMP_PATH,
                                                        version=RELATION_VERSION))
    tmp_off_line_relations = spark.read.parquet(
        '{path}/{version}/tmp_off_line_companys'.format(path=TMP_PATH,
                                                        version=RELATION_VERSION))
    
    tid_off_line_relations = off_line_relations.join(
        tmp_off_line_relations,
        off_line_relations.source_bbd_id == tmp_off_line_relations.tmp_company_name,
        'left_outer'
    ).where(
        filt(off_line_relations.relation_type, 
             tmp_off_line_relations.tmp_company_name,
             off_line_relations.source_isperson)
    ).select(
        off_line_relations.columns
    )
    
    tid_yisi = raw_yisi
    
    # 根据疑似数据，替换人的ID
    off_line_relations_with_yisi = tid_off_line_relations.join(
        tid_yisi,
        [tid_off_line_relations.source_name == tid_yisi.personName, 
         tid_off_line_relations.destination_bbd_id == tid_yisi.qyxxId],
        'left_outer'
    ).select(
        'company_name',
        'bbd_qyxx_id',
        'source_name',
        fun.when(
            tid_yisi.newGroupId.isNotNull(),
            tid_yisi.newGroupId.alias('source_bbd_id')
        ).otherwise(
            tid_off_line_relations.source_bbd_id
        ).alias('source_bbd_id'),
        'source_degree',
        'source_isperson',
        'destination_name',
        'destination_bbd_id',
        'destination_degree',
        'destination_isperson',
        'relation_type',
        'position'
    ).cache()
    
    off_line_relations_with_yisi.createOrReplaceTempView(
        'off_line_relations_with_yisi')
    
    spark.sql(
        '''
        insert 
        overwrite table wanxiang.off_line_relations partition (dt={version})
        select * from off_line_relations_with_yisi
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
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    
    IN_PATH = '/user/wanxiang/20180108wanxiangfromc5/'
    TMP_PATH = '/user/wanxiang/tmpdata/'
    OUT_PATH = '/user/wanxiang/step_two/'

    #sparkSession
    spark = get_spark_session()
    
    run()