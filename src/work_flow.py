# -*- coding: utf-8 -*-
"""
在 src 路径下执行 nohup python work_flow.py {version} &
离线数据生成好后在 HDFS 文件系统中创建一个文件夹让另一个节点能够知晓，并启动 index_success_check.sh 脚本
"""

import sys
import subprocess


def execute_some_step(step_name, file_name, 
                      xgxx_relation, relation_version):
    """
    提交某个spark-job
    """
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
    """
    根据计算结果判断是否出错，如果出错则退出程序并打印错误信息
    """
    if result:
        print "\n******************************\n"
        # 将错误信息发送到个人微信
        subprocess.call(
            '''
            curl --request GET --url "https://sc.ftqq.com/{SCKEY}.send?text={step_name}_{file_name}_has_an_error!";
            '''.format(SCKEY='SCU18362T36dadf900509742623c554ff37500c765a37802f84f04',
                       step_name=step_name,
                       file_name=file_name),
            shell=True
        )
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


def step_eight():
    result_eight = execute_some_step('step_eight', 'contact_node_and_edge.py', 
                                     XGXX_RELATION, RELATION_VERSION)
    is_success(result_eight, 
               'result_eight', 'contact_node_and_edge.py', RELATION_VERSION)


def run():

    subprocess.call(
        '''
        curl --request GET --url "https://sc.ftqq.com/{SCKEY}.send?text=开始生成离线数据";
        '''.format(SCKEY='SCU18362T36dadf900509742623c554ff37500c765a37802f84f04'),
        shell=True
    )

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
    step_eight()
# ==============================================================================
#     to_local()
# ==============================================================================

    # 上面所有步骤成功后，在下面的目录下创建一个文件夹，表示离线数据已经生成
    # 离线专用加载的节点探测到这个目录下有文件夹生成，就开始新一轮的 getmerge 操作
#==============================================================================
#     WORK_HOME = "/data8/wanxiang/zhaoyunfeng"
#     subprocess.call(
#         '''
#         hadoop fs -rmr hdfs:///user/wanxiang/offline_signal/{RELATION_VERSION};
#         hadoop fs -mkdir hdfs:///user/wanxiang/offline_signal/{RELATION_VERSION};
#         bash {WORK_HOME}/Wanxiang/src/index_success_check.sh {RELATION_VERSION};
#         '''.format(WORK_HOME=WORK_HOME, RELATION_VERSION=RELATION_VERSION),
#         shell=True
#     )
# 
#     subprocess.call(
#         '''
#         curl --request GET --url "https://sc.ftqq.com/{SCKEY}.send?text=开始get_merge";
#         '''.format(SCKEY='SCU18362T36dadf900509742623c554ff37500c765a37802f84f04'),
#         shell=True
#     )
#==============================================================================


if __name__ == '__main__':
    
    # 本地项目路径
    #  1、注意是否是计算历史数据，IS_HISTORY很关键，涉及到使用不同的数据库
    #  2、如果是离线计算‘实时图库’：XGXX_RELATION为版本号，且RELATION_VERSION=XGXX_RELATION
    #  3、如果是离线计算‘历史图库’：RELATION_VERSION为版本号，XGXX_RELATION取最新的版本
    IN_PATH = './'
    LOCAL_DATA_PATH = '/data8/wanxiang/zhaoyunfeng/data/'
    if len(sys.argv) == 2:
        RELATION_VERSION = sys.argv[1]
        XGXX_RELATION = sys.argv[1]
    elif len(sys.argv) == 3:
        RELATION_VERSION = sys.argv[1]
        XGXX_RELATION = sys.argv[2]
    else:
        RELATION_VERSION = '20180322'
        XGXX_RELATION = '20180322'
    
    IS_HISTORY = False
    
    run()
