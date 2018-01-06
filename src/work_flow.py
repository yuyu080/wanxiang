# -*- coding: utf-8 -*-

import os
import sys
import subprocess

def execute_some_step(step_name, file_name, 
                      xgxx_relation, relation_version):
    '''提交某个spark-job'''
    if IS_HISTORY:
        database = 'dw'
    else:
        database = 'wanxiang'
        
    execute_result = subprocess.call(
        '''
        /opt/spark-2.0.2/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 15g \
        --queue project.wanxiang \
        {path}/{file_name} {in_version} {out_version} {database}
        '''.format(path=IN_PATH+step_name, 
                   file_name=file_name,
                   database=database,
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

def step_zero():
    result_zero = execute_some_step('step_zero', 'real_off_line_relations.py',
                                    XGXX_RELATION, RELATION_VERSION)
    is_success(result_zero, 
               'step_zero', 'real_off_line_relations.py', RELATION_VERSION)
    
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
                path='/user/wanxiang/',
                step_name=step_name,
                version=RELATION_VERSION,
                file_name=file_name,
                local_path=LOCAL_DATA_PATH
            )
        )
    
    
    get_file('step_one', 'role_node')
    get_file('step_one', 'role_edge')
    get_file('step_one', 'isinvest_role_node')
    get_file('step_one', 'isinvest_role_edge')
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
    
def run():

    # 实时关联方与历史关联方存在不同的库，因此需要单独区分
    # 实时关联方需要新增一个流程，及解析关联方数据
    if not IS_HISTORY:
        step_zero()

    step_one()
    step_two()
    step_three()
    step_four()
    step_five()
    step_six()
    step_seven()
    to_local()
    
if __name__ == '__main__':
    
    # 本地项目路径
    IN_PATH = './'
    LOCAL_DATA_PATH = '/data8/wanxiang/zhaoyunfeng/data/'
    RELATION_VERSION = '20171219'
    XGXX_RELATION = '20171229'
    
    IS_HISTORY=True
    
    run()