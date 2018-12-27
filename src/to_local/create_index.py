# -*- coding: utf-8 -*-
"""
执行创建索引的命令，执行完后启动检查索引是否建好的脚本 start_check_index.sh
"""

import subprocess
import sys

if __name__ == '__main__':

    version = sys.argv[1]
    NEO4J_HOME = "/home/wanxiangneo4jpre/neo4j-enterprise-3.4.7"
    WORK_HOME = "/home/wanxiangneo4jpre"
    
    print("start to create indexes")
    
    flag = subprocess.call(
        '''
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Company(bbd_qyxx_id)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Person(bbd_qyxx_id)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Role(bbd_role_id)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Event(bbd_event_id)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Region(region_code)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Industry(industry_code)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Time(time)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Company(address)";
        {NEO4J_HOME}/bin/cypher-shell \
        -a bolt://{neo4j_ip}:{blot_port} \
        -u {user} -p {ps} "CREATE INDEX ON :Contact(bbd_contact_id)";
        '''.format(NEO4J_HOME=NEO4J_HOME,
                neo4j_ip='10.28.62.49',
                blot_port='30053',
                user='neo4j',
                ps='JNSWLOD2I2QNKB'),
        shell=True
    )
    
    #==============================================================================
    # subprocess.call(
    #     '''
    #     cd {WORK_HOME}/Wanxiang/src/to_local;
    #     nohup bash ./start_check_index.sh {version} &
    #     '''.format(WORK_HOME=WORK_HOME, version=version),
    #     shell=True
    # )
    #==============================================================================
    
    if flag != 0:
        sys.exit(
            '''
            创建索引失败，请检查图库是否启动、配置是否正确，然后手动创建索引
            '''
        )
