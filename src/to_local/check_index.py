# -*- coding: UTF-8 -*-
"""
每分钟被执行一次，检查索引是否全部建好。
如果全部建好，则在 HDFS 文件系统创建一个文件夹，让另外的节点上的脚本能够知晓索引已建好。
"""

from neo4j.v1 import GraphDatabase
import sys
import subprocess
import time

conn_addr = "bolt://10.28.62.46:30050"
user = "neo4j"
passwd = "fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC"


driver = GraphDatabase.driver(conn_addr, auth=(user, passwd))

cypher = """
call db.indexes
"""


with driver.session() as session:
    with session.begin_transaction() as tx:
        try:
            s = tx.run(cypher)
            flag = True
            today = time.strftime("%Y-%m-%d", time.localtime())
            version = sys.argv[1]
            for i in s:
                record = dict(i)
                state = record.get('state', '')
                if state != 'ONLINE':
                    flag = False
            if flag:
                subprocess.call(
                    '''
                    hadoop fs -mkdir hdfs://bbd43/tmp/success_{version};
                    /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/neo4j stop;
                    '''.format(version=version),
                    shell=True)
            else:
                sys.exit(1)
        except Exception as e:
            print(e)
