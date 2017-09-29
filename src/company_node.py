# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
company_node.py {version}
'''

import os
import re
from functools import partial

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

def spark_data_flow():
    '''
    STEP 1：数据准备
    '''
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())

    # 自然人节点
    person_df = spark.read.csv(
        '{path}/{version}/person_node'.format(
        path=OUT_PATH,
        version=RELATION_VERSION))
    
    # basic
    # 特别注意：要从basic里面剔除自然人节点，真是醉了
    raw_basic_df =spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        if(ipo_company = 'null', null, ipo_company) ipo_company,
        regcap_amount,
        realcap_amount,
        cast(esdate as string) esdate,
        regexp_replace(regexp_replace(address,',',''),'\"','') address,
        enterprise_status,
        company_province,
        company_county,
        company_industry,
        regexp_replace(regexp_replace(company_type,',',''),'\"','') company_type,
        company_gis_lat,
        company_gis_lon
        FROM
        dw.qyxx_basic
        WHERE
        dt='{version}'  
        '''.format(version=basic_version)
    )
    # 数据清洗, 该中间结果很重要，是后续构造节点的关键,因此需要落地
    os.system(
        '''
        hadoop fs -rmr {path}/prd_basic_df/{version}
        '''.format(path=TMP_PATH, 
                   version=RELATION_VERSION)
    )    

    prd_basic_df = raw_basic_df.join(
        person_df,
        person_df['_c0'] == raw_basic_df.bbd_qyxx_id,
        'left_outer'
    ).where(
        person_df['_c0'].isNull()
    ).select(
        raw_basic_df.columns
    ).na.fill(
        {'regcap_amount': 0, 'realcap_amount': 0,
         'company_gis_lat': 0, 'company_gis_lon': 0,
         'ipo_company': '-'}
    ).fillna(
        '-'
    ).where(
        filter_chinaese_udf('bbd_qyxx_id')
    ).where(
        filter_comma_udf('bbd_qyxx_id')
    ).dropDuplicates(
        ['bbd_qyxx_id']
    ).cache()
    
    prd_basic_df.coalesce(
        500
    ).write.parquet(
        ("{path}/"
         "prd_basic_df/"
         "{version}").format(path=TMP_PATH, 
                             version=RELATION_VERSION)
    )
        
    # black
    black_count_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        count(*) black_num
        FROM
        dw.black_list
        WHERE
        dt='{version}'  
        GROUP BY 
        bbd_qyxx_id
        '''.format(version=black_version)
    )
    
    # state_owned
    so_count_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        count(*) so_num
        FROM
        dw.qyxx_state_owned_enterprise_background
        WHERE
        dt='{version}'  
        GROUP BY 
        bbd_qyxx_id
        '''.format(version=state_owned_version)
    )
    
    # 节点的统计属性需要与相关信息节点对应起来，因此这里直接使用事件节点的中间结果来统计
    tid_xgxx_relation_df = spark.read.parquet(
        '{path}/tid_xgxx_relation_df/{version}'.format(path=TMP_PATH,
                                                       version=XGXX_RELATION)
    )
    all_xgxx_info_df = tid_xgxx_relation_df.groupBy(
        ['bbd_qyxx_id', 'bbd_table']
    ).agg(
        {'bbd_xgxx_id': 'count'}
    ).select(
        'bbd_qyxx_id',
        'bbd_table',
        fun.concat_ws(
            ':', 'bbd_table', 'count(bbd_xgxx_id)'
        ).alias('each_xgxx_info')
    ).groupBy(
        ['bbd_qyxx_id']
    ).agg(
        {'each_xgxx_info': 'collect_list'}
    ).withColumnRenamed(
        'collect_list(each_xgxx_info)', 'all_xgxx_info'
    )
    
    # bgxx
    bgxx_count_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        count(*) change_num
        FROM
        dw.qyxx_bgxx
        WHERE
        dt='{version}' 
        GROUP BY 
        bbd_qyxx_id
        '''.format(version=bgxx_version)
    )
    
    # 分支机构
    fzjg_count_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        count(*) fzjg_num
        FROM
        dw.qyxx_fzjg_extend
        WHERE
        dt='{version}'
        GROUP BY 
        bbd_qyxx_id
        '''.format(version=fzjg_version)
    )
    
    # 地域映射表
    mapping_df = spark.read.csv(
        '{path}/{file_name}'.format(path=TMP_TWO_PATH, 
                                    file_name=FILE_NAME),
        sep='\t',
        header=True
    ).withColumnRenamed(
        'code', 'company_county'
    ).dropDuplicates(
        ['company_county']
    )

    '''
    STEP 2.0 合并中间结果，由于涉及到多列解析，因此用rdd来输出最终结果
    '''
    tid_df = prd_basic_df.join(
        black_count_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        so_count_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        all_xgxx_info_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        bgxx_count_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        fzjg_count_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        mapping_df,
        'company_county',
        'left_outer'
    ).withColumn(
        'create_time', fun.unix_timestamp()
    ).withColumn(
        'update_time', fun.unix_timestamp()
    ).cache()
    
    def get_company_info(row):
        '''
        将节点的属性按照一定方式组合，并且输出
        '''
        row = row.asDict()
        
        def get_some_xgxx_info(xgxx_name, all_xgxx=row['all_xgxx_info']):
            if all_xgxx:
                for each_item in all_xgxx:
                    if xgxx_name in each_item:
                        return each_item.split(':')[1]
                else:
                    return '0'
            else:
                return '0'
        
        result = [
            row['bbd_qyxx_id'],
            'false',
            'true' if row['black_num'] else 'false',
            'true' if row['ipo_company'] != '-' else 'false',
            row['company_name'],
            'true' if row['so_num'] else 'false',
            row['esdate'],
            get_some_xgxx_info('ktgg'),
            get_some_xgxx_info('zgcpwsw'),
            get_some_xgxx_info('rmfygg'),
            get_some_xgxx_info('xzcf'),
            get_some_xgxx_info('zhixing'),
            get_some_xgxx_info('dishonesty'),
            get_some_xgxx_info('xgxx_shangbiao'),
            get_some_xgxx_info('shgy_zhongbjg'),
            get_some_xgxx_info('shgy_zhaobjg'),
            get_some_xgxx_info('qyxx_zhuanli'),
            get_some_xgxx_info('qyxg_qyqs'),
            str(row['change_num']) if row['change_num'] else '0',
            get_some_xgxx_info('recruit'),
            str(row['fzjg_num']) if row['fzjg_num'] else '0',
            str(row['company_gis_lon']),
            str(row['company_gis_lat']),
            get_some_xgxx_info('qyxg_jyyc'),
            row['address'],
            row['enterprise_status'].replace(',', u'\uff0c'),
            row['province'] if row['province'] else '-',
            row['city'] if row['city'] else '-',
            row['county'] if row['county'] else '-',
            row['company_industry'],
            row['company_type'].replace(',', u'\uff0c'),
            str(row['regcap_amount']),
            str(row['realcap_amount']),
            str(row['create_time']),
            str(row['update_time']),
            'Entity;Company'
        ]
        
        return ','.join(
            map(
                lambda r: r.replace(',', u'\uff0c'), result
            )
        )
    
    prd_rdd = tid_df.rdd.map(
        get_company_info
    )

    return prd_rdd

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
        .appName("wanxiang_company_node") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    
def run():
    prd_rdd = spark_data_flow()
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/company_node
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_rdd.saveAsTextFile(
        '{path}/{version}/company_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))

    
if __name__ == '__main__':
    # 输入参数
    RELATION_VERSION = '20170924'
    XGXX_RELATION = '20170927'
    FILE_NAME = 'company_county_mapping_20170524.data'
    TMP_PATH = '/user/antifraud/graph_relation_construction'
    TMP_TWO_PATH = '/user/antifraud/source/company_county_mapping'
    OUT_PATH = '/user/antifraud/source/tmp_test/tmp_file'
    
    basic_version = '20170927'
    black_version = '20170927'
    state_owned_version = '20170927'
    ktgg_version = '20170927'
    zgcpwsw_version = '20170927'
    rmfygg_version = '20170927'
    xzcf_version = '20170927'
    zhixing_version = '20170927'
    dishonesty_version = '20170927'
    shangbiao_version = '20170927'
    zhongbiao_version = '20170927'
    zhaobiao_version = '20170927'
    zhuanli_version = '20170927'
    tax_version = '20170927'
    bgxx_version = '20170927'
    recruit_version = '20170927'
    fzjg_version = '20170927'
    jyyc_version = '20170927'

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    