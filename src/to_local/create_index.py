# -*- coding: utf-8 -*-

import subprocess
import sys

version = sys.argv[1]

subprocess.call(
    '''
    ./start_check_index.sh {version}
    '''.format(version=version),
    shell=True
)

subprocess.call(
    '''
    /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Company(bbd_qyxx_id);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Person(bbd_qyxx_id);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Role(bbd_role_id);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Event(bbd_event_id);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Region(region_code);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Industry(industry_code);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Time(time);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Company(address);";
     /data1/wanxiangneo4jpre/neo4j-enterprise-3.4.0/bin/cypher-shell \
     -a bolt://10.28.62.46:30050 \
     -u neo4j -p fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfC "CREATE INDEX ON :Contact(bbd_contact_id);";
    '''
)
