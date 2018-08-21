# -*- coding: utf-8 -*-
"""
执行创建索引的命令，执行完后启动检查索引是否建好的脚本 start_check_index.sh
"""

import subprocess
import sys

version = sys.argv[1]

print("start to create indexes")

flag = subprocess.call(
    '''
    /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Company(bbd_qyxx_id)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Person(bbd_qyxx_id)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Role(bbd_role_id)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Event(bbd_event_id)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Region(region_code)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Industry(industry_code)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Time(time)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Company(address)";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Contact(bbd_contact_id)";
    ''',
    shell=True
)

subprocess.call(
    '''
    cd /data1/wanxiangneo4jpre/Wanxiang/src/to_local;
    nohup bash ./start_check_index.sh {version} &
    '''.format(version=version),
    shell=True
)

if flag != 0:
    sys.exit(
        '''
        创建索引失败，请检查图库是否启动、配置是否正确，然后手动创建索引
        '''
    )
