# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
event_node_and_edge.py {xgxx_relation} {relation_version}
'''
import sys
import os
import re
import datetime
import time
import hashlib
from functools import partial


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun, types as tp, DataFrame


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

def get_type(*label):
    '''获得节点或边的类型'''
    return ';'.join([
            each_label.lower().capitalize()
            for each_label in label
    ])

def get_timestamp(date):
    '''将日期转换成linux时间戳'''
    try:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
        return long(time.mktime(date_obj.timetuple()))
    except:
        try:
            date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            return long(time.mktime(date_obj.timetuple()))
        except:
            return 0

def get_standard_date(date):
    '''将日期转换成标准格式'''
    try:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        return date_obj.strftime('%Y-%m-%d')
    except:
        return date

def get_xgxx_id(*cols):
    xgxx_id = hashlib.md5(''.join(map(lambda s: s.encode('utf-8'), cols)))
    return xgxx_id.hexdigest()


def raw_spark_data_flow():
    '''
    STEP 0. 创建table_list与col_dict, 明确需要哪些输入表
    ''' 
    
    # 原始相关信息
    xgxx_relation_df = spark.sql(
        '''
        select * from ods.xgxx_relation where dt='{version}'
        '''.format(version=XGXX_RELATION)
    )
    
    os.system(
        '''
        hadoop fs -rmr {path}/xgxx_relation_df/{version}
        '''.format(path=TMP_PATH, 
                   version=RELATION_VERSION)
    )
    xgxx_relation_df.coalesce(
        300
    ).write.parquet(
        ('{path}/'
         'xgxx_relation_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION)
    )
    xgxx_relation_df = spark.read.parquet(
        ('{path}/'
         'xgxx_relation_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION)
    )
    
    
    table_df = xgxx_relation_df.select(
        'bbd_table'
    ).dropDuplicates(
        ['bbd_table']
    ).cache()
    
    # 包含每个表event字段的df
    table_envnt_date_df = spark.read.csv(
        '{path}/{file_name}'.format(path=IN_PATH,
                                    file_name=FILE_NAME),
        sep='\t'
    ).fillna(
        u'无'
    ).withColumnRenamed(
        '_c0', 'bbd_table'
    ).withColumnRenamed(
        '_c1', 'event_time'
    ).distinct(
    ).cache(
    )
    
    # xgxx表
    # table_list = table_df.rdd.map(
    #     lambda r: r.bbd_table
    # ).collect(
    # )
    table_list = [
        'baidu_news'
       ,'dcos'
       ,'dishonesty'
       ,'ktgg'
       ,'qylogo'
       ,'qyxg_circxzcf'
       ,'qyxg_jyyc'
       ,'qyxg_qyqs'
       ,'qyxg_yuqing'
       ,'qyxg_yuqing_main'
       ,'qyxx_finance_xkz'
       ,'qyxx_wanfang_zhuanli'
       ,'recruit'
       ,'rjzzq'
       ,'rmfygg'
       ,'sfpm_taobao'
       ,'shgy_tdcr'
       ,'shgy_zhaobjg'
       ,'shgy_zhongbjg'
       ,'simutong'
       ,'tddkgs'
       ,'tddy'
       ,'tdzr'
       ,'xgxx_shangbiao'
       ,'xzcf'
       ,'zgcpwsw'
       ,'zhixing'
       ,'zhuanli_zhuanyi'
       ,'zpzzq'
       ,'qyxx_bgxx'
       ,'qyxx_liquidation'
       ,'qyxx_sharesfrost'
       ,'qyxg_xzxk'
       ,'qyxx_sharesimpawn'
       ,'qyxx_mordetail'
       ,'domain_name_website_info'
       ,'overseas_investment'
       ,'qyxx_nb_jbxx'
       ,'qyxx_nb_gzsm'
       ,'qyxx_nb_czxx'
       ,'qyxx_nb_wzxx'
       ,'qyxx_nb_fzjg'
       ,'qyxx_nb_tzxx'
       ,'qyxx_nb_zcxx'
       ,'qyxx_nb_dbxx'
       ,'qyxx_nb_xgxx'
       ,'qyxx_nb_xzxk'
       ,'qyxx_nb_bgxx'
    ]
    
    #需要被剔除的表(节点)
    filter_list = [
        'qyxg_wdzj',
        'qyxg_wdty',
        'qyxg_jijin_relate',
        'qyxg_jijin_simu',
        'qyxg_exchange',
        'jijinye_info',
        'qyxg_platform_data',
        'qyxg_zhongchou',
        'qyxx_miit_jlzzdwmd',
        'qyxx_jzsgxkz',
        'qyxx_hzp_pro_prod_cert',
        'qyxx_tk',
        'qyxx_enterprisequalificationforeign',
        'qyxx_gcjljz',
        'ssgs_zjzx',
        'qyxx_nyscqyzzcx',
        'qyxx_medi_pro_prod_cert',
        'qyxx_medi_jy_prod_cert',
        'qyxx_industrial_production_permit',
        'qyxx_haiguanzongshu',
        'qyxx_gmpauth_prod_cert',
        'qyxx_food_prod_cert',
        'qyxx_ck',
        'qyxg_yuqing_main_hj',
        'qyxx_zhongdeng',
        'zuzhijigoudm',
        'qyxg_zzjgdm',
        'qyxx_zhuanli',
        'black_list'
    ]
    
    # 表+时间字段
    table_dict = dict(
        table_envnt_date_df.fillna(
            u'无'
        ).replace(
            u'无', ''
        ).rdd.map(
            lambda r: (r.bbd_table, r.event_time)
        ).collect(
        )
    )

    return table_list, filter_list, table_dict


def tmp_spark_data_flow(TABLE_DICT):
    '''
    STEP 1. 将某些没有xgxx_id，或者某些特殊的“事件”格式化成xgxx_relation的表结构
    '''
    get_xgxx_id_udf = fun.udf(get_xgxx_id, tp.StringType())
    
    def get_additional_xgxx_df(version, table_name, columns):
        raw_df = spark.sql(
            """
            SELECT
            '' id,
            '{table_name}' bbd_table,
            0 id_type,
            dt,
            {event_time} event_time,
            {col}
            FROM
            ods.{table_name}
            WHERE
            dt='{version}'
            """.format(version=version, 
                       table_name=table_name, 
                       event_time=TABLE_DICT.get(table_name, ''),
                       col=','.join(
                           map(lambda s: "cast({0} as string) {0}".format(s), 
                               columns))
                      )
        ).fillna(
            ''
        )
        
        tid_df = raw_df.select(
            'id',
            'bbd_qyxx_id',
            get_xgxx_id_udf(*columns).alias('bbd_xgxx_id'),
            'bbd_table',
            'id_type',
            fun.current_timestamp().alias('create_time'),
            'dt',
            'event_time'
        )
        
        return tid_df
    
    
    
    # qyxx_bgxx
    qyxx_bgxx_columns = ['bbd_qyxx_id', 'change_date', 'change_items', 
                         'content_before_change', 'content_after_change']
    tmp_xgxx_relation_df_1 = get_additional_xgxx_df(XGXX_RELATION, 'qyxx_bgxx', 
                                                    qyxx_bgxx_columns)
    
    # qyxx_liquidation
    qyxx_liquidation_columns = ['bbd_qyxx_id', 'company_name', 'ligentity', 
                                'ligprincipal', 'liqmen', 'ligst', 
                                'ligenddate', 'debttranee', 'claimtranee']
    tmp_xgxx_relation_df_2 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_liquidation', 
                                                    qyxx_liquidation_columns)
    
    # qyxx_sharesfrost
    qyxx_sharesfrost_columns = ['bbd_qyxx_id', 'company_name', 'frodocno', 
                                'froauth', 'frofrom', 'froto', 'froam', 
                                'thawauth', 'thawdocno', 'thawdate']
    tmp_xgxx_relation_df_3 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_sharesfrost', 
                                                    qyxx_sharesfrost_columns)
    
    # qyxx_sharesimpawn
    qyxx_sharesimpawn_columns = ['bbd_qyxx_id', 'company_name' , 'imporg', 
                                 'imporgtype', 'impam', 'imponrecdate', 
                                 'impexaeep', 'impsandate', 'impto', 
                                 'morregcno', 'imporg_idno', 'pledgee',
                                 'pledgee_idno', 'impstate', 'impsituation']
    tmp_xgxx_relation_df_4 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_sharesimpawn', 
                                                    qyxx_sharesimpawn_columns)
    
    # qyxx_mordetail
    qyxx_mordetail_columns = ['bbd_qyxx_id', 'company_name', 'morreg_id', 
                              'mortgagor', 'more', 'regorg', 'regidate', 
                              'mortype', 'morregcno', 'appregrea', 
                              'priclaseckind', 'priclasecam', 'pefperform', 
                              'pefperto', 'candate', 'guaname', 'guadetali']
    tmp_xgxx_relation_df_5 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_mordetail', 
                                                    qyxx_mordetail_columns)
    
    # black_list
    # 由于具有单独的属性，因此独立计算
  
    # 中间数据落地
    
    tmp_xgxx_relation_df = tmp_xgxx_relation_df_1.union(
        tmp_xgxx_relation_df_2
    ).union(
        tmp_xgxx_relation_df_3
    ).union(
        tmp_xgxx_relation_df_4
    ).union(
        tmp_xgxx_relation_df_5
    ).dropDuplicates(
        ['bbd_xgxx_id', 'bbd_table']
    )
    
    os.system(
        ("hadoop fs -rmr " 
        "{path}/"
        "tmp_xgxx_relation_df/{version}").format(path=TMP_PATH, 
                                                 version=RELATION_VERSION))
    
    tmp_xgxx_relation_df.coalesce(
        300
    ).write.parquet(
        ("{path}/"
         "tmp_xgxx_relation_df/{version}"
        ).format(path=TMP_PATH, 
                 version=RELATION_VERSION)
    )


def tid_spark_data_flow(table_list, filter_list, table_dict):
    '''
    STEP 2. 构建具体事件的df，并将其合并，获取event_time
    '''
    get_standard_date_udf = fun.udf(get_standard_date, tp.StringType())
    
    def get_df(table_name, version):
        '''根据某表是否有事件时间，选择不同的读取方式'''
        try:
            if table_dict[table_name]:
                df = spark.sql(
                    '''
                    SELECT
                    bbd_xgxx_id,
                    {event_time} event_time,
                    '{table_name}' bbd_table
                    FROM
                    ods.{table_name}
                    WHERE
                    dt='{version}'
                    '''.format(
                        table_name=table_name,
                        event_time=table_dict[table_name],
                        version=version
                    )
                )
            else:
                df = spark.sql(
                    '''
                    SELECT
                    bbd_xgxx_id,
                    '0' event_time,
                    '{table_name}' bbd_table
                    FROM
                    ods.{table_name}
                    WHERE
                    dt='{version}'
                    '''.format(
                        table_name=table_name,
                        version=version
                    )
                )
            return df
        except:
            return table_name
        
    
    def union_df(table_list, filter_list, version):
        df_list = []
        for each_table in table_list:
            if each_table not in filter_list:
                each_df = get_df(each_table, version)
                if isinstance(each_df, DataFrame):
                    df_list.append(each_df)
        
        #将多个df合并
        tid_df = eval(
            "df_list[{0}]".format(0) + 
            "".join([
                    ".union(df_list[{0}])".format(df_index) 
                    for df_index in range(1, len(df_list))])
        )
    
        return tid_df

    # 数据落地
    raw_event_df = union_df(table_list, filter_list, XGXX_RELATION).fillna(
        '0'
    ).dropDuplicates(
        ['bbd_xgxx_id', 'bbd_table']
    ).select(
        'bbd_xgxx_id',
        get_standard_date_udf('event_time').alias('event_time'),
        'bbd_table',
    )

    os.system(
        ("hadoop fs -rmr " 
        "{path}/"
        "raw_event_df/{version}").format(path=TMP_PATH, 
                                         version=RELATION_VERSION))
    
    raw_event_df.coalesce(
        300
    ).write.parquet(
        ("{path}/"
         "raw_event_df/{version}"
        ).format(path=TMP_PATH, 
                 version=RELATION_VERSION)
    )
        

def prd_spark_data_flow():
    '''
    STEP 3.0 raw_event_df与xgxx_relation作join明确每个关系的时间
    '''
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    get_timestamp_udf = fun.udf(get_timestamp, tp.LongType())
    
    # 相关信息
    xgxx_relation_df = spark.read.parquet(
        ('{path}/'
         'xgxx_relation_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION)
    )    

    # 事件时间
    raw_event_df = spark.read.parquet(
        ('{path}/'
         'raw_event_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION)
    )    
        
    # 额外的相关信息
    tmp_xgxx_relation_df = spark.read.parquet(
        ('{path}/'
         'tmp_xgxx_relation_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION)
    )
    
    # 合并
    os.system(
        ("hadoop fs -rmr " 
        "{path}/"
        "tid_xgxx_relation_df/{version}").format(path=TMP_PATH, 
                                         version=RELATION_VERSION))
        
    tid_xgxx_relation_df = xgxx_relation_df.join(
        raw_event_df,
        ['bbd_xgxx_id', 'bbd_table']
    ).select(
        xgxx_relation_df['id'],
        xgxx_relation_df.bbd_qyxx_id,
        xgxx_relation_df.bbd_xgxx_id,
        xgxx_relation_df.bbd_table,
        xgxx_relation_df.id_type,
        xgxx_relation_df.create_time,
        xgxx_relation_df.dt,
        raw_event_df.event_time
    ).cache()
    
    tid_xgxx_relation_df.union(
        tmp_xgxx_relation_df
    ).withColumn(
        'event_timestamp', get_timestamp_udf('event_time')
    ).where(
        filter_comma_udf('bbd_xgxx_id')
    ).where(
        filter_chinaese_udf('bbd_xgxx_id')
    ).where(
        filter_comma_udf('bbd_qyxx_id')
    ).where(
        filter_chinaese_udf('bbd_qyxx_id')
    ).dropDuplicates(
        ['bbd_xgxx_id', 'bbd_qyxx_id']
    ).coalesce(
        100
    ).write.parquet(
        ('{path}/'
         'tid_xgxx_relation_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION)
    )


def prd_spark_graph_data_flow():
    '''
    获取角色节点
    '''
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    get_Event_udf = fun.udf(
        partial(get_type, 'Entity', 'Event'), 
        tp.StringType()
    )    
    get_event_relation_udf = fun.udf(lambda r: r.upper(), tp.StringType())
    
    tid_xgxx_relation_df = spark.read.parquet(
        ('{path}/'
         'tid_xgxx_relation_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION))
    
    # 事件节点
    prd_event_nodes_df = tid_xgxx_relation_df.select(
        tid_xgxx_relation_df.bbd_xgxx_id.alias(
            'bbd_event_id:ID'
        ),
        tid_xgxx_relation_df.event_timestamp.alias(
            'event_time:long'
        ),
        fun.unix_timestamp(
        ).alias(
            'create_time:long'
        ),
        fun.unix_timestamp(
        ).alias(
            'update_time:long'
        ),
        get_Event_udf(
            'bbd_table'
        ).alias(
            ':LABEL'
        )    
    ).where(
        filter_chinaese_udf('bbd_event_id:ID')
    ).where(
        filter_comma_udf('bbd_event_id:ID')
    ).dropDuplicates(
        ['bbd_event_id:ID']
    )

    # 事件关系
    prd_event_edge_df = tid_xgxx_relation_df.select(
        tid_xgxx_relation_df.bbd_qyxx_id.alias(
            ':START_ID'
        ),    
        fun.unix_timestamp(
        ).alias(
            'create_time:long'
        ),
        'id_type',
        tid_xgxx_relation_df.bbd_xgxx_id.alias(
            ':END_ID'
        ),
        get_event_relation_udf(
            'bbd_table'
        ).alias(':TYPE')
    )
    
    return prd_event_nodes_df, prd_event_edge_df
    

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
        .appName("wanxiang_event_node_and_edge") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    
    
def run():
    '''
    前三步在准备数据，事件节点的中间数据很重要，在后面也会用到
    由于balck_list事件比较特殊，因此需要单独计算
    '''
    TABLE_LIST, FILTER_LIST, TABLE_DICT = raw_spark_data_flow()
    tmp_spark_data_flow(TABLE_DICT)
    tid_spark_data_flow(TABLE_LIST, FILTER_LIST, TABLE_DICT)
    prd_spark_data_flow()
    prd_event_nodes_df, prd_event_edge_df = prd_spark_graph_data_flow()    
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/event_node
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_event_nodes_df.coalesce(600).write.csv(
        '{path}/{version}/event_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))    

    os.system(
        '''
        hadoop fs -rmr {path}/{version}/event_edge
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_event_edge_df.coalesce(600).write.csv(
        '{path}/{version}/event_edge'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
    
if __name__ == '__main__':
    # 输入参数
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    
    FILE_NAME = 'raw_graph_event_col_20171024.data'
    IN_PATH = '/user/wanxiang/inputdata/'
    TMP_PATH = '/user/wanxiang/tmpdata/'
    OUT_PATH = '/user/wanxiang/step_two/'

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    