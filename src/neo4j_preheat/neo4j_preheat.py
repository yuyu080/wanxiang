# -*- coding: utf-8 -*-
"""
对数据进行预热，提前加载数据到内存中
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from neo4j.v1 import GraphDatabase


def get_path(driver, bbd_qyxx_id):
    """
    获取单个节点的关系
    """
    with driver.session(max_retry_time=3) as session:
        with session.begin_transaction() as tx:
            edges = tx.run(
                '''
                match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-
                [:INVEST|SUPERVISOR|DIRECTOR|LEGAL|EXECUTIVE|BRANCH*1..8]-(b) 
                return p
                ''',
                bbd_qyxx_id=bbd_qyxx_id)
    return edges


def get_path_data(iterator):
    my_driver = GraphDatabase.driver(URI, auth=AUTH)
    for each_bbd_qyxx_id in iterator:
        paths = get_path(my_driver, each_bbd_qyxx_id)
        yield len(paths.data())


def spark_data_flow():
    tar_company = spark.sparkContext.textFile(IN_FILE, 
                                              minPartitions=1000)

    prd_rdd = tar_company.map(
        lambda r: r.split(',')[0]
    ).mapPartitions(
        get_path_data
    )
    
    return prd_rdd


def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "25g")
    conf.set("spark.executor.instances", 10)
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
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 
    

def run():
    result = spark_data_flow()
    data = result.take(300)
    for each_item in data:
        print each_item


if __name__ == '__main__':
    # 输入参数
    URI = 'bolt://10.28.52.151:7690'
    AUTH = ("neo4j", "fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfB")
    IN_FILE = '/user/wanxiang/inputdata/raw_wanxiang_preheat_node_20171122.data'
    
    # sparkSession
    spark = get_spark_session()
    
    run()
