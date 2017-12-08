# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
role_node_and_edge.py {xgxx_relation} {relation_version} {database}
'''

import sys
import os
import re
from functools import partial
import hashlib

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

def get_id(src, des, relation):
    '''
    生成规则：md5（起点name+终点id+关系类型）
    由于源ID有中文，因此这里需要做容错
    '''
    try:
        role_id = hashlib.md5(src.encode('utf-8') + 
                              des.encode('utf-8') +
                              relation.encode('utf-8'))
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
    
    get_relation_label_3_udf = fun.udf(
        partial(lambda r: r, 'VIRTUAL'), tp.StringType())    
    
    raw_role_df = spark.sql(
        '''
        SELECT 
        source_bbd_id           b,
        destination_bbd_id      c,
        source_name             b_name,
        destination_name        c_name,
        upper(relation_type)    bc_relation,
        position
        FROM 
        {database}.off_line_relations 
        WHERE 
        dt='{version}'  
        AND
        (source_isperson = 0 or source_isperson = 1)
        AND
        (destination_isperson = 0 or destination_isperson = 1)
        '''.format(database=DATABASE,
                   version=RELATION_VERSION)
    ).where(
        filter_comma_udf('b_name')
    ).where(
        filter_comma_udf('c_name')
    ).dropDuplicates(
        ['b', 'c', 'bc_relation']
    ).cache()
    
    # Isinvest： 自定义的虚拟role节点
    tid_isinvest_role_df = raw_role_df.groupBy(
        ['b', 'c', 'b_name', 'c_name']
    ).agg(
        {'bc_relation': 'collect_set'}
    ).select(
        'b',
        get_isinvest_id_udf('b_name', 'c').alias('bbd_role_id:ID'),
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
    ).dropDuplicates(
        ['bbd_role_id:ID']
    )
    
    # role：角色节点
    tid_role_df = raw_role_df.select(
        'b',
        get_id_udf('b_name', 'c', 'bc_relation').alias('bbd_role_id:ID'),
        'c',
        'bc_relation',
        'position'
    ).where(
        filter_comma_udf('b')
    ).where(
        filter_chinaese_udf('b')
    ).where(
        filter_comma_udf('c')
    ).where(
        filter_chinaese_udf('c')
    ).cache()
    
    # 给投资节点加上投资比例，其余节点的比例为空
    raw_qyxx_gdxx_ratio = spark.sql(
        '''
        SELECT
        shareholder_id b,
        shareholder_name b_name,
        bbd_qyxx_id c,
        'INVEST' bc_relation,
        invest_ratio ratio
        FROM
        dw.qyxx_gdxx
        WHERE
        dt='{version}'
        '''.format(version=XGXX_RELATION)
    ).select(
        'b',
        get_id_udf('b_name', 'c', 'bc_relation').alias('bbd_role_id:ID'),
        'c',
        'bc_relation',
        'ratio'
    ).replace(
        "null", '-'
    ).dropDuplicates(
        ['bbd_role_id:ID']
    )
    
    # 加上“分支机构”这一新的角色节点
    raw_qyxx_fzjg_merge = spark.sql(
        '''
        SELECT
        bbd_branch_id b,
        bbd_qyxx_id c,
        name b_name,
        company_name c_name,
        'BRANCH' bc_relation,
        '分支机构' role_name,
        '-' ratio
        FROM
        dw.qyxx_fzjg_merge
        WHERE
        dt='{version}'
        '''.format(version=XGXX_RELATION)
    ).dropDuplicates(
        ['b', 'c', 'bc_relation']
    )
    
    tid_qyxx_fzjg_merge = raw_qyxx_fzjg_merge.where(
        "b != 'None'"
    ).where(
        filter_comma_udf('b')
    ).where(
        filter_chinaese_udf('b')
    ).where(
        filter_comma_udf('c')
    ).where(
        filter_chinaese_udf('c')
    ).where(
        filter_comma_udf('b_name')
    ).where(
        filter_comma_udf('c_name')
    ).select(
        'b',
        'b_name',
        get_id_udf('b_name', 'c', 'bc_relation').alias('bbd_role_id:ID'),
        'c',
        'c_name',
        'bc_relation',
        'role_name',
        'ratio',
    )
    
    # 中间结果落地，为了将fzjg中的企业加入company_node中
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "tid_qyxx_fzjg_merge/{version}").format(path=TMP_PATH, 
                                                 version=RELATION_VERSION))
    tid_qyxx_fzjg_merge.write.parquet(
        ('{path}/'
         'tid_qyxx_fzjg_merge/'
         '{version}').format(path=TMP_PATH, 
                             version=RELATION_VERSION))
    
    prd_qyxx_fzjg_merge = tid_qyxx_fzjg_merge.select(
        'bbd_role_id:ID',
        'role_name',
        'ratio',
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),
        get_role_label_udf('bc_relation').alias(':LABEL')
    )

    # 生成最终角色节点
    prd_role_node_df = tid_role_df.where(
        tid_role_df['bbd_role_id:ID'] != ''
    ).join(
        raw_qyxx_gdxx_ratio,
        'bbd_role_id:ID',
        'left_outer'
    ).select(
        tid_role_df['bbd_role_id:ID'],
        tid_role_df.position.alias('role_name'),
        raw_qyxx_gdxx_ratio.ratio.alias("ratio"),
        fun.unix_timestamp().alias('create_time:long'),
        fun.unix_timestamp().alias('update_time:long'),    
        get_role_label_udf(tid_role_df['bc_relation']).alias(':LABEL')
    ).union(
        prd_qyxx_fzjg_merge
    ).fillna(
        '-'
    ).replace(
        '', '-'
    ).replace(
        'null', '-'
    ).dropDuplicates(
        ['bbd_role_id:ID']
    ).cache()

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
    ).dropDuplicates(
        [':START_ID', ':END_ID']
    )
    
    # role：角色节点的关系
    # 将所有关系融合
    tid_all_role_df = tid_role_df.union(
        tid_qyxx_fzjg_merge.select(
            'b',
            'bbd_role_id:ID',
            'c',
            'bc_relation',
            'role_name'
        )
    ).cache()
        
    prd_role_edge_df = tid_all_role_df.select(
        tid_all_role_df.b.alias(':START_ID'), 
        tid_all_role_df['bbd_role_id:ID'].alias(':END_ID'),
        fun.unix_timestamp().alias('create_time:long'),
        tid_all_role_df.bc_relation.alias(':TYPE')
    ).union(
        tid_all_role_df.select(
            tid_all_role_df['bbd_role_id:ID'].alias(':START_ID'), 
            tid_all_role_df.c.alias(':END_ID'),
            fun.unix_timestamp().alias('create_time:long'),
            tid_all_role_df.bc_relation.alias(':TYPE')
        )
    ).dropDuplicates(
        [':START_ID', ':END_ID']
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
        .appName("wanxiang_role_node_and_edge") \
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
    prd_isinvest_role_node_df.coalesce(600).write.csv(
        '{path}/{version}/isinvest_role_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
    os.system(
        '''
        hadoop fs -rmr {path}/{version}/role_node
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_role_node_df.coalesce(600).write.csv(
        '{path}/{version}/role_node'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))    

    os.system(
        '''
        hadoop fs -rmr {path}/{version}/isinvest_role_edge
        '''.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    prd_isinvest_role_edge_df.coalesce(600).write.csv(
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
    ).coalesce(600).write.csv(
        '{path}/{version}/role_edge'.format(
            path=OUT_PATH,
            version=RELATION_VERSION))
    
if __name__ == '__main__':
    # 输入参数
    XGXX_RELATION = sys.argv[1]
    RELATION_VERSION = sys.argv[2]
    DATABASE = sys.argv[3]
    TMP_PATH = '/user/wanxiang/tmpdata/'
    OUT_PATH = '/user/wanxiang/step_one/'

    #sparkSession
    spark = get_spark_session()
    
    run()
    
    