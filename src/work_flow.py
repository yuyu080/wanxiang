# -*- coding: utf-8 -*-

import os
import sys
import subprocess

def execute_some_step(step_name, file_name, xgxx_relation, relation_version):
    '''提交某个spark-job'''
    execute_result = subprocess.call(
        '''
        /opt/spark-2.0.2/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 15g \
        --queue project.wanxiang \
        {path}/{file_name} {in_version} {out_version}
        '''.format(path=IN_PATH+step_name, 
                   file_name=file_name,
                   in_version=xgxx_relation,
                   out_version=relation_version),
        shell=True
    )
    return execute_result

def is_success(result, step_name, file_name, relation_version):
    '''根据计算结果判断是否出错，如果出错则退出程序并打印错误信息'''
    if result:
        print "\n******************************\n"
        sys.exit(
            '''
            {step_name}|{file_name}|{out_version} \
            has a error !!!
            '''.format(
                step_name=step_name,
                file_name=file_name,
                out_version=relation_version
            )
        )

def step_one():
    result_one = execute_some_step('step_one', 'role_node_and_edge.py', 
                                   XGXX_RELATION, RELATION_VERSION)
    is_success(result_one, 
               'step_one', 'role_node_and_edge.py', RELATION_VERSION)
    
def step_two():
    result_two = execute_some_step('step_two', 'event_node_and_edge.py', 
                                   XGXX_RELATION, RELATION_VERSION)
    is_success(result_two, 
               'step_two', 'event_node_and_edge.py', RELATION_VERSION)
    
def step_three():
    result_three = execute_some_step('step_three', 'person_node.py', 
                                   XGXX_RELATION, RELATION_VERSION)
    is_success(result_three, 
               'step_three', 'person_node.py', RELATION_VERSION)
    
def step_four():
    result_four = execute_some_step('step_four', 'company_node.py', 
                                   XGXX_RELATION, RELATION_VERSION)
    is_success(result_four, 
               'step_four', 'company_node.py', RELATION_VERSION)

def step_five():
    result_five = execute_some_step('step_five', 'region_node_and_edge.py', 
                                   XGXX_RELATION, RELATION_VERSION)
    is_success(result_five, 
               'step_five', 'region_node_and_edge.py', RELATION_VERSION)

def step_six():
    result_six = execute_some_step('step_six', 'industry_node_and_edge.py', 
                                   XGXX_RELATION, RELATION_VERSION)
    is_success(result_six, 
               'step_six', 'industry_node_and_edge.py', RELATION_VERSION)

def step_seven():
    result_seven = execute_some_step('step_seven', 'time_node_and_edge.py', 
                                   XGXX_RELATION, RELATION_VERSION)
    is_success(result_seven, 
               'step_seven', 'time_node_and_edge.py', RELATION_VERSION)


def to_local():
    pass


def run():
    step_one()
#==============================================================================
#     step_two()
#     step_three()
#     step_four()
#     step_five()
#     step_six()
#     step_seven()
#==============================================================================

if __name__ == '__main__':
    
    # 本地项目路径
    IN_PATH = './'
    
    RELATION_VERSION = '20171018'
    XGXX_RELATION = '20171101'

    run()