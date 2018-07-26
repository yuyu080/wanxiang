# 万象离线加载流程

## 离线图库加载流程
#### 1、确定work_flow.py脚本参数，是计算历史数据还是重置，注意修改数据版本参数，详见脚本注释。
#### 2、切换至专用离线图库（地址见http://git.bbdops.com/yuyu080/wanxiang_neo4j/tree/master ） 加载环境，注意节点磁盘使用情况，一次加载需要预留1T磁盘空间
#### 3、修改to_local.py脚本路径与数据版本参数，将数据下载至Neo4j的import目录
#### 4、在import相应的数据日期目录下执行数据加载命令，填写相应的数据版本号：
nohup ../../bin/neo4j-admin import \
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
--ignore-extra-columns=true \
--report-file=import.report &
#### 5、加载完毕后，启动neo4j服务，依次执行下面语句建索引，执行完毕后在图库中通过 call db.index 查看索引创建结果：
CREATE INDEX ON :Company(bbd_qyxx_id)   
CREATE INDEX ON :Person(bbd_qyxx_id)   
CREATE INDEX ON :Role(bbd_role_id)   
CREATE INDEX ON :Event(bbd_event_id)   
CREATE INDEX ON :Region(region_code)   
CREATE INDEX ON :Industry(industry_code)   
CREATE INDEX ON :Time(time)   
CREATE INDEX ON :Company(address)   
CREATE INDEX ON :Contact(bbd_contact_id)   
#### 6、通过 call db.index 查看索引创建结果，在确认索引全部创建成功后，执行bin/neo4j stop停止数据库
#### 7、将加载好的数据库scp至灰度环境，并启动，通知测试、产品验证
#### 8、验证通过后，通知运维人员进行上线准备，具体线上切换时间由产品确认


## 离线历史图库加载流程

#### 1、确定需要更新的数据版本月份（例如：要生成7月份图库，那么就从数仓选取7月末的某一个数据版本）
#### 2、确定版本号修改work_flow中的参数，修改规则详见脚本注释， IS_HISTORY设置为False
#### 3、计算、生成数据库文件方式同离线图库加载流程
#### 4、找产品、运维确认上线时间、情况，待历史库加载完成后，更新redis中的元数据：删除老板数据库的地址，插入新数据库的地址，保留6个版本。例如：
import redis   
pool = redis.ConnectionPool(host='10.28.60.15', port=26382,    
                            password='wanxiang', db=0)   
r = redis.Redis(connection_pool=pool)   
r.set('wx_neo4j_his_info.201806', 'bolt://10.28.62.206:7687')   
r.delete('wx_neo4j_his_info.201712')   

## redis缓存加载流程

#### 1、由数据产品确定mysql数据库地址，并修改脚本参数
#### 2、在C6上执行 balck_list_to_redis.py 脚本，数据版本号与离线加载的xgxx版本相同，此操作务必小心，需要确认是否有加载到数据
#### 3、cache_to_redis.py 缓存万象的指标计算结果，每天更新一次，可查看 cache_to_redis.sh脚本
