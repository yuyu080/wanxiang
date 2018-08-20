# -*- coding: utf-8 -*-
"""
Created on Thu Mar 08 18:44:26 2018

@author: Administrator
"""

import os
import sys
import subprocess


def to_local():
    
    def get_file(step_name, file_name):
        local_file = LOCAL_DATA_PATH+file_name
        if not os.path.exists(LOCAL_DATA_PATH):
            os.makedirs(LOCAL_DATA_PATH)
        if os.path.exists(local_file):
            os.remove(local_file)

        flag = os.system(
            '''
            hadoop fs -getmerge \
            {path}/{step_name}/{version}/{file_name}/* \
            {local_path}/{version}/{file_name}.data
            '''.format(
                path='hdfs://bbd43/user/wanxiang/',
                step_name=step_name,
                version=RELATION_VERSION,
                file_name=file_name,
                local_path=LOCAL_DATA_PATH
            )
        )
        if flag != 0:
            raise Exception

    get_file('step_one', 'role_node')
    get_file('step_one', 'role_edge')
    # get_file('step_one', 'isinvest_role_node')
    # get_file('step_one', 'isinvest_role_edge')
    print "step_one sucess !!"

    get_file('step_two', 'event_node')
    get_file('step_two', 'event_edge')
    print "step_two sucess !!"
    
    get_file('step_three', 'person_node')
    print "step_three sucess !!"
    
    get_file('step_four', 'company_node')
    print "step_four sucess !!"
    
    get_file('step_five', 'region_node')
    get_file('step_five', 'region_edge')
    print "step_five sucess !!"
    
    get_file('step_six', 'industry_node')
    get_file('step_six', 'industry_edge')
    print "step_six sucess !!"
    
    get_file('step_seven', 'time_node')
    get_file('step_seven', 'time_edge')
    print "step_seven sucess !!"
    
    get_file('step_eight', 'phone_node')  
    get_file('step_eight', 'phone_edge')
    get_file('step_eight', 'email_node')
    get_file('step_eight', 'email_edge')
    print "step_eight sucess !!"


LOCAL_DATA_PATH = '/data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/import/'

if len(sys.argv) == 2:
    RELATION_VERSION = sys.argv[1]
    XGXX_RELATION = sys.argv[1]
else:
    RELATION_VERSION = '20180627'
    XGXX_RELATION = '20180627'


try:
    # 把 HDFS 上的文件 getmerge 到 Neo4j 的 import 目录下
    to_local()
    # 把 header 文件拷贝到相应目录下
    flag1 = subprocess.call(
        '''
        cp ./header/* \
        /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/import/{version}
        '''.format(version=RELATION_VERSION),
        shell=True
    )

    # 开始执行 import
    flag2 = subprocess.call(
        '''
        cd /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/import/{version};
        /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/neo4j-admin import \
        --database graph_{version}.db \
        --nodes person_node.header,person_node.data \
        --nodes role_node.header,role_node.data \
        --nodes event_node.header,event_node.data \
        --nodes company_node.header,company_node.data \
        --nodes region_node.header,region_node.data \
        --nodes industry_node.header,industry_node.data \
        --nodes time_node.header,time_node.data \
        --nodes phone_node.header,phone_node.data \
        --nodes email_node.header,email_node.data \
        --relationships role_edge.header,role_edge.data \
        --relationships event_edge.header,event_edge.data \
        --relationships region_edge.header,region_edge.data \
        --relationships industry_edge.header,industry_edge.data \
        --relationships time_edge.header,time_edge.data \
        --relationships email_edge.header,email_edge.data \
        --relationships phone_edge.header,phone_edge.data \
        --ignore-missing-nodes=true \
        --ignore-duplicate-nodes=true \
        --quote=︻ \
        --high-io=true \
        --report-file=import.report > \
        process.log
        '''.format(version=RELATION_VERSION),
        shell=True
    )
    if flag1 == flag2 == 0:
        subprocess.call(
            '''
            mv /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/conf/neo4j.conf \
            /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/conf/neo4j.conf.bak;
            sed '1c dbms.active_database=graph_{version}.db' \
            /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/conf/neo4j.conf.bak > \
            /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/conf/neo4j.conf;
            cat /dev/null > /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/logs/neo4j.log;
            /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/neo4j restart;
            sleep 120;
            python ./create_index.py {version} > create_index.log 2>&1
            '''.format(version=RELATION_VERSION),
            shell=True
        )
    else:
        sys.exit(
            '''
            import 失败了!
            '''
        )
except:
    sys.exit(
        '''
        to_local 失败了!
        '''
    )

