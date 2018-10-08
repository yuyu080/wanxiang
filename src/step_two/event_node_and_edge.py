# -*- coding: utf-8 -*-
"""
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 15g \
--queue project.wanxiang \
event_node_and_edge.py {xgxx_relation} {relation_version}
"""
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
    """
    ID中逗号或为空值，则将该记录删除
    """
    if not col or ',' in col or u'\uff0c' in col:
        return False
    else:
        return True


def filter_chinaese(col):
    """
    字段中只要包含中文，将其过滤
    """
    if col:
        match = re.search(ur'[\u4e00-\u9fa5]', col)
        return False if match else True
    else:
        return False


def get_type(*label):
    """
    获得节点或边的类型
    """
    return ';'.join([
            each_label.lower().capitalize()
            for each_label in label
    ])


def get_timestamp(date):
    """
    将日期转换成linux时间戳
    """
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
    """
    将日期转换成标准格式
    """
    try:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        return date_obj.strftime('%Y-%m-%d')
    except:
        return date


def get_xgxx_id(bbd_qyxx_id, bbd_unique_id):
    xgxx_id = '{}_{}'.format(bbd_qyxx_id, bbd_unique_id)
    return xgxx_id


def new_to_old(bbd_table):
    bbd_table_dict = {"legal_dishonest_persons_subject_to_enforcement": "dishonesty",
                      "legal_persons_subject_to_enforcement": "zhixing",
                      "legal_court_notice": "rmfygg"}
    return bbd_table_dict.get(bbd_table, bbd_table)


def raw_spark_data_flow():
    """
    STEP 0. 创建table_list与col_dict, 明确需要哪些输入表
    """
    
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
        'dcos',
        'ktgg',
        'overseas_investment',
        'qylogo',
        'qyxg_qyqs',
        'qyxg_yuqing',
        'qyxx_finance_xkz',
        'qyxx_wanfang_zhuanli',
        'recruit',
        'rjzzq',
        'sfpm_taobao',
        'shgy_tdcr',
        'shgy_zhaobjg',
        'shgy_zhongbjg',
        'tddkgs',
        'tddy',
        'xgxx_shangbiao',
        'xzcf',
        'zgcpwsw',
        'zpzzq',
        'qyxx_bgxx',
        'qyxx_liquidation',
        'qyxx_sharesfrost',
        'qyxg_xzxk',
        'qyxx_sharesimpawn',
        'qyxx_mordetail',
        'domain_name_website_info',
        'qyxg_debet',
        'qyxx_annual_report_jbxx',
        'qyxg_bmcprz',
        'qyxg_ccjc',
        'qyxg_china_land_tdcz',
        'qyxg_dgjwxk',
        'qyxg_environment_label',
        'qyxg_gtfwjk',
        'qyxg_jtyszljl',
        'qyxg_medicinal_deal',
        'qyxg_medicinal_info',
        'qyxg_yzwf',
        'qyxx_ck',
        'qyxx_enterprisequalificationforeign',
        'qyxx_food_prod_cert',
        'qyxx_gcjljz',
        'qyxx_gmpauth_prod_cert',
        'qyxx_hzp_pro_prod_cert',
        'qyxx_industrial_production_permit',
        'qyxx_medi_jy_prod_cert',
        'qyxx_medi_pro_prod_cert',
        'qyxx_miit_jlzzdwmd',
        'qyxx_tk'
        'qyxg_yzwf'
    ]


    # 需要被剔除的表(节点)
    filter_list = [
        ''
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
    """
    STEP 1. 将某些没有xgxx_id，或者某些特殊的“事件”格式化成xgxx_relation的表结构
    """
    get_xgxx_id_udf = fun.udf(get_xgxx_id, tp.StringType())
    
    def get_additional_xgxx_df(version, table_name):
        raw_df = spark.sql(
            """
            SELECT
            '' id,
            bbd_qyxx_id,
            bbd_unique_id,
            '{table_name}' bbd_table,
            0 id_type,
            dt,
            CAST({event_time} AS string) event_time,
            company_name
            FROM
            dw.{table_name}
            WHERE
            dt='{version}'
            """.format(version=version, 
                       table_name=table_name, 
                       event_time=TABLE_DICT.get(table_name, ''))
        ).fillna(
            ''
        ).replace(
            'null', ''
        ).replace(
            'NULL', ''
        )
        
        tid_df = raw_df.select(
            'id',
            'bbd_qyxx_id',
            get_xgxx_id_udf('bbd_qyxx_id',
                            'bbd_unique_id').alias('bbd_xgxx_id'),
            'bbd_table',
            'id_type',
            'dt',
            'event_time',
            'company_name'
        )
        
        return tid_df

    # qyxx_bgxx
    tmp_xgxx_relation_df_1 = get_additional_xgxx_df(XGXX_RELATION, 'qyxx_bgxx')
    
    # qyxx_liquidation
    tmp_xgxx_relation_df_2 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_liquidation')
    
    # qyxx_sharesfrost
    tmp_xgxx_relation_df_3 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_sharesfrost')
    
    # qyxx_sharesimpawn
    tmp_xgxx_relation_df_4 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_sharesimpawn')
    
    # qyxx_mordetail
    tmp_xgxx_relation_df_5 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_mordetail')

    # qyxx_jyyc
    tmp_xgxx_relation_df_6 = get_additional_xgxx_df(XGXX_RELATION, 
                                                    'qyxx_jyyc')
    
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
    ).union(
        tmp_xgxx_relation_df_6    
    ).fillna(
        '0'
    ).fillna(
        0
    ).dropDuplicates(
        ['bbd_qyxx_id', 'bbd_xgxx_id', 'bbd_table']
    )
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/" 
         "tmp_xgxx_relation_df/{version}").format(path=TMP_PATH,
                                                  version=RELATION_VERSION))
    
    tmp_xgxx_relation_df.where(
        tmp_xgxx_relation_df.event_time <= FORMAT_RELATION_VERSION
    ).coalesce(
        300
    ).write.parquet(
        ("{path}/"
         "tmp_xgxx_relation_df/{version}"
         ).format(path=TMP_PATH,
                  version=RELATION_VERSION)
    )


def rel_event_data_flow():
    """
    STEP 2. 获取新表的事件信息
    """
    get_standard_date_udf = fun.udf(get_standard_date, tp.StringType())
    new_to_old_udf = fun.udf(new_to_old, tp.StringType())

    legal_enterprise_rel_df = spark.sql(
        """
        SELECT
        '' id,
        bbd_qyxx_id,
        bbd_xgxx_id,
        bbd_table,
        0 id_type,
        dt,
        CAST(sort_date AS string) event_time,
        entity_name company_name
        FROM
        dw.{table_name}
        WHERE
        dt='{version}'
        """.format(version=XGXX_RELATION,
                   table_name='legal_enterprise_rel')
    ).fillna(
        '0'
    ).fillna(
        0
    ).replace(
        'null', ''
    ).replace(
        'NULL', ''
    ).dropDuplicates(
        ['bbd_qyxx_id', 'bbd_xgxx_id', 'bbd_table']
    ).select(
        'id',
        'bbd_qyxx_id',
        'bbd_xgxx_id',
        new_to_old_udf('bbd_table').alias('bbd_table'),
        'id_type',
        get_standard_date_udf('event_time').alias('event_time'),
        'dt',
        'company_name'
    )

    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "legal_enterprise_rel_df/{version}").format(path=TMP_PATH,
                                                     version=RELATION_VERSION))

    legal_enterprise_rel_df.where(
        legal_enterprise_rel_df.event_time <= FORMAT_RELATION_VERSION
    ).coalesce(
        300
    ).write.parquet(
        ("{path}/"
         "legal_enterprise_rel_df/{version}"
         ).format(path=TMP_PATH,
                  version=RELATION_VERSION)
    )


def tid_spark_data_flow(table_list, filter_list, table_dict):
    """
    STEP 3. 构建具体事件的df，并将其合并，获取event_time
    """
    get_standard_date_udf = fun.udf(get_standard_date, tp.StringType())
    
    def get_df(table_name, version):
        """
        根据某表是否有事件时间，选择不同的读取方式
        """
        try:
            if table_dict[table_name]:
                df = spark.sql(
                    '''
                    SELECT
                    '' id,
                    '{table_name}' bbd_table,
                    0 id_type,
                    bbd_xgxx_id,
                    bbd_qyxx_id,
                    dt,
                    CAST({event_time} AS string) event_time,
                    '-' company_name
                    FROM
                    dw.{table_name}
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
                    '' id,
                    '{table_name}' bbd_table,
                    id_type,
                    bbd_xgxx_id,
                    bbd_qyxx_id,
                    dt,
                    '0' event_time,
                    '-' company_name
                    FROM
                    dw.{table_name}
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
        
        # 将多个df合并
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
    ).fillna(
        0
    ).replace(
        'null', ''
    ).replace(
        'NULL', ''
    ).dropDuplicates(
        ['bbd_qyxx_id', 'bbd_xgxx_id', 'bbd_table']
    ).select(
        'id',
        'bbd_qyxx_id',
        'bbd_xgxx_id',
        'bbd_table',
        'id_type',
        get_standard_date_udf('event_time').alias('event_time'),
        'dt',
        'company_name'
    )

    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "raw_event_df/{version}").format(path=TMP_PATH,
                                          version=RELATION_VERSION))
    
    # 根据关联方日期来过滤事件，以此得到历史的事件节点
    raw_event_df.where(
        raw_event_df.event_time <= FORMAT_RELATION_VERSION
    ).coalesce(
        1000
    ).write.parquet(
        ("{path}/"
         "raw_event_df/{version}"
         ).format(path=TMP_PATH,
                  version=RELATION_VERSION)
    )
        

def prd_spark_data_flow():
    """
    STEP 4.0 raw_event_df与xgxx_relation作join明确每个关系的时间
    """
    filter_chinaese_udf = fun.udf(filter_chinaese, tp.BooleanType())
    filter_comma_udf = fun.udf(filter_comma, tp.BooleanType())
    get_timestamp_udf = fun.udf(get_timestamp, tp.LongType())

    # 事件时间, 以及相关信息
    raw_event_df = spark.read.parquet(
        ('{path}/'
         'raw_event_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION))
        
    # 额外的相关信息
    tmp_xgxx_relation_df = spark.read.parquet(
        ('{path}/'
         'tmp_xgxx_relation_df/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION))

    # 替换成新表的相关信息
    legal_enterprise_rel_df = spark.read.parquet(
        ('{path}/'
         'legal_enterprise_rel_df/'
         '{version}').format(path=TMP_PATH,
                             version=RELATION_VERSION))

    
    # 合并
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "tid_xgxx_relation_df/{version}").format(path=TMP_PATH,
                                                  version=RELATION_VERSION))
        
    tid_xgxx_relation_df = raw_event_df.select(
        raw_event_df['id'],
        raw_event_df.bbd_qyxx_id,
        raw_event_df.bbd_xgxx_id,
        raw_event_df.bbd_table,
        raw_event_df.id_type,
        raw_event_df.dt,
        raw_event_df.event_time
    ).cache()
    
    tid_xgxx_relation_df.union(
        tmp_xgxx_relation_df.select(
            tmp_xgxx_relation_df['id'],
            tmp_xgxx_relation_df.bbd_qyxx_id,
            tmp_xgxx_relation_df.bbd_xgxx_id,
            tmp_xgxx_relation_df.bbd_table,
            tmp_xgxx_relation_df.id_type,
            tmp_xgxx_relation_df.dt,
            tmp_xgxx_relation_df.event_time
        )
    ).union(
        legal_enterprise_rel_df.select(
            legal_enterprise_rel_df['id'],
            legal_enterprise_rel_df.bbd_qyxx_id,
            legal_enterprise_rel_df.bbd_xgxx_id,
            legal_enterprise_rel_df.bbd_table,
            legal_enterprise_rel_df.id_type,
            legal_enterprise_rel_df.dt,
            legal_enterprise_rel_df.event_time
        )
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
                             version=RELATION_VERSION))


def prd_spark_graph_data_flow():
    """
    获取角色节点
    """
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
    # 由于有很多无效的qyxx_id,因此这里可以先将这些关系剔除，后期通过streaming更新
    prd_event_edge_df = tid_xgxx_relation_df.where(
        tid_xgxx_relation_df.bbd_qyxx_id != '0'
    ).select(
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
    conf.set("spark.executor.memory", "45g")
    conf.set("spark.executor.instances", 30)
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
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    
    
def run():
    """
    前四步在准备数据，事件节点的中间数据很重要，在后面也会用到
    由于balck_list事件比较特殊，因此需要单独计算
    """
    TABLE_LIST, FILTER_LIST, TABLE_DICT = raw_spark_data_flow()
    tmp_spark_data_flow(TABLE_DICT)
    rel_event_data_flow()
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
    FORMAT_RELATION_VERSION = datetime.datetime.strptime(
        RELATION_VERSION, '%Y%m%d').strftime('%Y-%m-%d')
    
    FILE_NAME = 'raw_graph_event_col_20180204.data'
    IN_PATH = '/user/wanxiang/inputdata/'
    TMP_PATH = '/user/wanxiang/tmpdata/'
    OUT_PATH = '/user/wanxiang/step_two/'

    # sparkSession
    spark = get_spark_session()

    # 从各个事件表中读取事件信息， 将中间数据事件信息写入 /user/wanxiang/tmpdata/raw_event_df、
    # /user/wanxiang/tmpdata/legal_enterprise_rel_df 和 /user/wanxiang/tmpdata/tmp_xgxx_relation_df
    # 将上面三种事件合并后将中间数据写入 /user/wanxiang/tmpdata/tid_xgxx_relation_df
    # 上面的中间结果在创建公司节点及其属性时都会用到
    # 从 /user/wanxiang/tmpdata/tid_xgxx_relation_df 读取中间数据
    # 处理得到事件的 node 和 edge 写入 /user/wanxiang/step_two
    run()
