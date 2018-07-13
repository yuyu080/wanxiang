# -*- coding: utf-8 -*-
"""
Created on Thu Mar 08 18:44:26 2018

@author: Administrator
"""

import os


def to_local():
    
    def get_file(step_name, file_name):
        local_file = LOCAL_DATA_PATH+file_name
        if not os.path.exists(LOCAL_DATA_PATH):
            os.makedirs(LOCAL_DATA_PATH)
        if os.path.exists(local_file):
            os.remove(local_file)
            
        os.system(
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


LOCAL_DATA_PATH = '/data1/wanxiangneo4j/neo4j-enterprise-3.2.6/import/'
RELATION_VERSION = '20180627'
XGXX_RELATION = '20180627'

to_local()
