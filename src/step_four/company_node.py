# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
company_node.py {xgxx_relation} {relation_version}
'''

import sys
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

def get_label(x):
    try:
        return x.split(';')[2]
    except:
        return ''
        
def spark_data_flow():
    '''
    STEP 1：数据准备
    '''
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    get_label_udf = fun.udf(get_label , tp.StringType())
    
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
        regcap_currency,
        realcap_currency,
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
                                                       version=RELATION_VERSION)
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
    
    # 地域映射表
    mapping_df = spark.read.csv(
        '{path}/{file_name}'.format(path=IN_PATH, 
                                    file_name=FILE_NAME),
        sep='\t',
        header=True
    ).withColumnRenamed(
        'code', 'company_county'
    ).dropDuplicates(
        ['company_county']
    )

    # 中间结果
    tmp_role_df = spark.read.parquet(
        "{path}/tmp_role_df/{version}".format(path=TMP_PATH, 
                                              version=RELATION_VERSION))

    # fzjg
    tmp_fzjg_df = tmp_role_df.where(
        tmp_role_df.TYPE == 'OF'
    ).where(
        get_label_udf('START_LABEL') == 'Branch'
    ).groupBy(
        'END_ID'
    ).count(
    ).withColumnRenamed(
        'count', 'fzjg'
    ).withColumnRenamed(
        'END_ID', 'bbd_qyxx_id'
    ).cache()
    
    # dwtzxx
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
    
    # gdxx
    tmp_gdxx_df = tmp_role_df.where(
        tmp_role_df.TYPE == 'OF'
    ).where(
        get_label_udf('START_LABEL') == 'Invest'
    ).groupBy(
        'END_ID'
    ).count(
    ).withColumnRenamed(
        'count', 'gdxx'
    ).withColumnRenamed(
        'END_ID', 'bbd_qyxx_id'
    ).cache()
    
    # baxx
    tmp_baxx_df = tmp_role_df.where(
        tmp_role_df.TYPE == 'OF'
    ).where(
        fun.when(
            get_label_udf('START_LABEL') == 'Supervisor', True
        ).when(
            get_label_udf('START_LABEL') == 'Director', True
        ).when(
            get_label_udf('START_LABEL') == 'Executive', True
        ).otherwise(
            False
        )
    ).groupBy(
        'END_ID'
    ).count(
    ).withColumnRenamed(
        'count', 'baxx'
    ).withColumnRenamed(
        'END_ID', 'bbd_qyxx_id'
    ).cache()
    

    '''
    STEP 2.0 合并中间结果，由于涉及到多列解析，因此用rdd来输出最终结果
    '''
    tid_df = prd_basic_df.join(
        all_xgxx_info_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        so_count_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        tmp_fzjg_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        tmp_dwtzxx_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        tmp_gdxx_df,
        'bbd_qyxx_id',
        'left_outer'
    ).join(
        tmp_baxx_df,
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
    ).fillna(
        '-'
    ).replace(
        '', '-'
    ).replace(
        'null', '-'
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
            get_some_xgxx_info('qyxx_bgxx'),
            get_some_xgxx_info('recruit'),
            get_some_xgxx_info('qyxg_jyyc'),
            get_some_xgxx_info('black_list'),
            get_some_xgxx_info('sfpm_taobao'),
            get_some_xgxx_info('qyxx_liquidation'),
            get_some_xgxx_info('qyxx_sharesfrost'),
            str(row['fzjg']) if row['fzjg'] else '0',
            str(row['dwtzxx']) if row['dwtzxx'] else '0',
            str(row['gdxx']) if row['gdxx'] else '0',
            str(row['baxx']) if row['baxx'] else '0',
            get_some_xgxx_info('qyxg_xzxk'),
            get_some_xgxx_info('qyxx_sharesimpawn'),
            get_some_xgxx_info('qyxx_mordetail'),
            get_some_xgxx_info('qyxg_yuqing'),
            get_some_xgxx_info('rjzzq'),
            get_some_xgxx_info('zpzzq'),
            get_some_xgxx_info('domain_name_website_info'),
            get_some_xgxx_info('overseas_investment'),
            get_some_xgxx_info('qyxx_nb_jbxx'),
            str(row['company_gis_lon']),
            str(row['company_gis_lat']),
            row['address'],
            row['enterprise_status'].replace(',', u'\uff0c'),
            row['province'] if row['province'] else '-',
            row['city'] if row['city'] else '-',
            row['county'] if row['county'] else '-',
            row['company_industry'],
            row['company_type'].replace(',', u'\uff0c'),
            str(row['regcap_amount']),
            row['regcap_currency'],
            str(row['realcap_amount']),
            row['realcap_currency'],
            str(row['create_time']),
            str(row['update_time']),
            'Entity;Company'
        ]
        
        return ','.join(
            map(
                lambda r: r.replace(
                    ',', u'\uff0c'
                ).replace(
                    '\\', ''
                ).replace(
                    '"', ''
                ), result
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
    prd_rdd.coalesce(600).saveAsTextFile(
        '{path}/{version}/company_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))

    
if __name__ == '__main__':
    # 输入参数
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    FILE_NAME = 'company_county_mapping_20170524.data'
    IN_PATH = '/user/wanxiang/inputdata'
    TMP_PATH = '/user/wanxiang/tmpdata'
    OUT_PATH = '/user/wanxiang/step_one'
    
    basic_version = XGXX_RELATION
    black_version = XGXX_RELATION
    state_owned_version = XGXX_RELATION
    ktgg_version = XGXX_RELATION
    zgcpwsw_version = XGXX_RELATION
    rmfygg_version = XGXX_RELATION
    xzcf_version = XGXX_RELATION
    zhixing_version = XGXX_RELATION
    dishonesty_version = XGXX_RELATION
    shangbiao_version = XGXX_RELATION
    zhongbiao_version = XGXX_RELATION
    zhaobiao_version = XGXX_RELATION
    zhuanli_version = XGXX_RELATION
    tax_version = XGXX_RELATION
    bgxx_version = XGXX_RELATION
    recruit_version = XGXX_RELATION
    fzjg_version = XGXX_RELATION
    jyyc_version = XGXX_RELATION

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    