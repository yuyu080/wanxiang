# -*- coding: utf-8 -*-

import time
import string
from itertools import groupby
from operator import itemgetter
from collections import Counter

import redis
import pandas as pd
from neo4j.v1 import GraphDatabase
import networkx as nx

class Neo4jDriver(object):
    
    uri = 'bolt://10.28.102.32:7687'
    my_driver = GraphDatabase.driver(
        uri, auth=("neo4j", "fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfB"))

class redisHelper(object):
    pool = redis.ConnectionPool(host='10.28.100.24', port=36340, 
                                    password='BBDredis', db=0)
    client = redis.Redis(connection_pool=pool)
    pipline = client.pipeline(transaction=True)

class MyTimer(object):
    def __init__(self,verbose=False):
        self.verbose=verbose
 
    def __enter__(self):
        self.start=time.time()
        return self
 
    def __exit__(self,*unused):
        self.end=time.time()
        self.secs=self.end-self.start
        self.msecs=self.secs
        if self.verbose:
            print"elapsed time: %f s"%self.msecs

class RelationFeatureConstruction(object):
    '''
    计算特征的函数集
    '''
    
    def __init__(self, graph, bbd_qyxx_id):
        for node, attr in graph.nodes(data=True):
            if attr.get('bbd_qyxx_id', '') == bbd_qyxx_id:
                self.tar_id = node
        self.input_distance = nx.shortest_path_length(graph.to_undirected(), 
                                                     source=self.tar_id)     
        self.graph = graph
        
        # 目标公司董监高列表
        self.tar_director_node = []
        self.tar_executive_node = []
        self.tar_supervisor_node = []
        
        # 目标公司自然人股东
        self.tar_invest_inhuman = []
        
        # 一度关联方企业
        self.one_company_node = []
        
        # 指标值
        self.gs_eb_1_degree_rel_non_ls_num = 0
        self.gs_eb_2_degree_rel_non_ls_num = 0
        self.gs_eb_2_degree_rel_blackcomp = 0
        self.gs_eb_2_degree_rel_revoke_num = 0
        self.gs_eb_2_degree_rel_ltd_abo_bum = 0
        self.gs_eb_1_deg_lgl_per_rel_jd_num = 0
        self.gs_eb_1_degree_lg_pe_re_num_jd = 0
        self.gs_eb_2_degree_lg_pe_re_jd_num = 0
        self.gs_eb_2_degree_rel_exe_num = 0
        self.gs_eb_2_degree_rel_num_exe = 0
        self.gs_eb_threedegree_rel_exe_num = 0
        self.gs_eb_2_degree_lg_pe_re_rv_num = 0
        self.gs_rp_1_degree_rel_cnt = 0
        self.gs_rp_1_degree_rel_na_rt = []
        self.gs_rp_1_degree_rel_inv_cnt = 0
        self.gs_rp_1_degree_rel_patent_cnt = 0
        self.gs_rp_1_degree_rel_not_nainsp = set()
        self.gs_rp_2_degree_rel_cnt = []
        self.gs_rp_2_degree_rel_na_rt = []
        self.gs_rp_2_degree_rel_inv_cnt = 0
        self.gs_rp_1_degree_rel_non_nainsp = set()
        self.gs_rp_2_degree_rel_patent_cnt = 0
        self.gs_rp_leader_pluralism_cnt = []
        self.gs_rp_1_degree_listed_comp_cnt = 0
        self.gs_rp_2_degree_rel_na_cnt = 0
        self.gs_rp_3_degree_rel_na_cnt = 0
        
    def get_gs_eb_1_degree_rel_non_ls_num(self, node_attr):
        self.gs_eb_1_degree_rel_non_ls_num += node_attr
    
    def get_gs_eb_2_degree_rel_non_ls_num(self, node_attr):
        self.gs_eb_2_degree_rel_non_ls_num += node_attr
        
    def get_gs_eb_2_degree_rel_blackcomp(self, node_attr):
        if node_attr:
            self.gs_eb_2_degree_rel_blackcomp += 1
            
    def get_gs_eb_2_degree_rel_revoke_num(self, node_attr):
        if u'吊销' in node_attr:
            self.gs_eb_2_degree_rel_revoke_num += 1
    
    
    def get_gs_eb_2_degree_rel_ltd_abo_bum(self, node_attr):
        self.gs_eb_2_degree_rel_ltd_abo_bum += node_attr

    def get_gs_eb_1_deg_lgl_per_rel_jd_num(self, node_attr):
        self.gs_eb_1_deg_lgl_per_rel_jd_num += node_attr

    def get_gs_eb_1_degree_lg_pe_re_num_jd(self, node_attr):
        if node_attr:
            self.gs_eb_1_degree_lg_pe_re_num_jd += 1
    
    def get_gs_eb_2_degree_lg_pe_re_jd_num(self, node_attr):
        self.gs_eb_2_degree_lg_pe_re_jd_num += node_attr
    
    def get_gs_eb_2_degree_rel_exe_num(self, node_attr):
        self.gs_eb_2_degree_rel_exe_num += node_attr
        
    def get_gs_eb_2_degree_rel_num_exe(self, node_attr):
        if node_attr:
            self.gs_eb_2_degree_rel_num_exe += 1
        
    def get_gs_eb_threedegree_rel_exe_num(self, node_attr):
        self.gs_eb_threedegree_rel_exe_num += node_attr

    def get_gs_eb_2_degree_lg_pe_re_rv_num(self, node_attr):
        if u'吊销' in node_attr:
            self.gs_eb_2_degree_lg_pe_re_rv_num += 1
    
    def get_gs_rp_1_degree_rel_cnt(self, node_attr):
        self.gs_rp_1_degree_rel_cnt += 1
    
    def get_gs_rp_1_degree_rel_na_rt(self):
        count = Counter(self.gs_rp_1_degree_rel_na_rt)
        return count[True]*1. / (count[True]+count[False])
    
    def get_gs_rp_1_degree_rel_inv_cnt(self, node_attr):
        self.gs_rp_1_degree_rel_inv_cnt += node_attr
        
    def get_gs_rp_1_degree_rel_patent_cnt(self, node_attr):
        self.gs_rp_1_degree_rel_patent_cnt += node_attr
    
    def get_gs_rp_1_degree_rel_not_nainsp(self):
        return len(self.gs_rp_1_degree_rel_not_nainsp)
        
    def get_gs_rp_2_degree_rel_cnt(self):
        return len(self.gs_rp_2_degree_rel_cnt)
    
    def get_gs_rp_2_degree_rel_na_rt(self):
        count = Counter(self.gs_rp_2_degree_rel_na_rt)
        return count[True]*1. / (count[True]+count[False])
    
    def get_gs_rp_2_degree_rel_inv_cnt(self, node_attr):
        self.gs_rp_2_degree_rel_inv_cnt += node_attr
    
    def get_gs_rp_1_degree_rel_non_nainsp(self):
        return len(self.gs_rp_1_degree_rel_non_nainsp)
    
    def get_gs_rp_2_degree_rel_patent_cnt(self, node_attr):
        self.gs_rp_2_degree_rel_patent_cnt += node_attr
    
    def get_des_feature(self):
        '''
        目标公司董监高对外投资&兼职
        '''
        company_des_node = set()
        company_inv_node = set()
        for each_node in (self.tar_director_node+
                          self.tar_executive_node+
                          self.tar_supervisor_node):
            for each_edge in self.graph.out_edges(each_node, data=True):
                if (('DIRECTOR' in each_edge[2]['type'] or
                        'EXECUTIVE' in each_edge[2]['type'] or
                        'SUPERVISOR' in each_edge[2]['type']) and
                            self.tar_node != each_edge[1]):
                    company_des_node.add(each_edge[1])
                if ('INVEST' in each_edge[2]['type'] and 
                        self.tar_node != each_edge[1]):
                    company_inv_node.add(each_edge[1])
        return len(company_des_node), len(company_inv_node)
    
    def get_gs_rp_1_degree_listed_comp_cnt(self, node_attr):
        if node_attr:
            self.gs_rp_1_degree_listed_comp_cnt += 1
    
    def get_gs_rp_2_degree_rel_na_cnt(self, node_attr):
        self.gs_rp_2_degree_rel_na_cnt += 1
    
    def get_gs_rp_3_degree_rel_na_cnt(self, node_attr):
        self.gs_rp_3_degree_rel_na_cnt += 1
    
    def get_gs_rp_supervisor_cnt(self):
        return len(self.tar_supervisor_node)
    
    def get_company_des_featrure(self):
        '''
        一度公司董事，高管数
        '''
        director_node = set()
        all_des_node = set()
        for each_node in self.one_company_node:
            for each_edge in self.graph.in_edges(each_node, data=True):
                if 'DIRECTOR' in each_edge[2]['type']:
                    director_node.add(each_edge[0])
                if ('DIRECTOR' in each_edge[2]['type'] or
                        'EXECUTIVE' in each_edge[2]['type'] or
                        'SUPERVISOR' in each_edge[2]['type']):
                    all_des_node.add(each_edge[0])
        return len(director_node), len(all_des_node)
    
    def get_investor_inv(self):
        out_invest_degree = 0
        out_des_degree = 0
        for each_node in self.tar_invest_inhuman:
            for each_edge in self.graph.out_edges(each_node, data=True):
                if 'INVEST' in each_edge[2]['type']:
                    out_invest_degree += 1
            out_des_degree += self.graph.out_degree(each_node)
        return out_invest_degree, out_des_degree
                    
    
    
    
    
    
    def tar_node(self, node_attr):
        pass
    
    def get_relation_features(self):
        '''
        计算入口
        '''
        for node, pro in self.graph.node(data=True):
            if self.input_distance[node] == 3:
                if self.graph.node[node]['is_human']:
                    self.get_gs_rp_3_degree_rel_na_cnt(node)
                else:
                    self.get_gs_eb_threedegree_rel_exe_num(pro.get('zhixing', 
                                                                   0))
            elif self.input_distance[node] == 2:
                if self.graph.node[node]['is_human']:
                    self.get_gs_rp_2_degree_rel_na_cnt(node)
                else:
                    self.get_gs_eb_2_degree_rel_non_ls_num(pro.get('zgcpwsw', 
                                                                   0))
                    self.get_gs_eb_2_degree_rel_blackcomp(pro.get('is_black', 
                                                                   False))
                    self.get_gs_eb_2_degree_rel_revoke_num(pro.get('estatus', 
                                                                   ''))
                    self.get_gs_eb_2_degree_rel_ltd_abo_bum(pro.get('jyyc', 
                                                                    0))
                    self.get_gs_eb_2_degree_lg_pe_re_jd_num(pro.get('zgcpwsw', 
                                                                    0))
                    self.get_gs_eb_2_degree_rel_exe_num(pro.get('zhixing', 
                                                                0))
                    self.get_gs_eb_2_degree_rel_num_exe(pro.get('zhixing', 
                                                                0))
                    self.gs_rp_1_degree_rel_non_nainsp.add(pro.get('company_industry',
                                                                   ''))
                    self.get_gs_eb_2_degree_lg_pe_re_rv_num(pro.get('estatus',
                                                                    ''))
                    self.get_gs_rp_2_degree_rel_patent_cnt(pro.get('zhuanli',
                                                                   0))
                    
                self.gs_rp_2_degree_rel_cnt.append(node)
                self.gs_rp_2_degree_rel_na_rt.append(pro.get('is_human', False))
                self.get_gs_rp_2_degree_rel_inv_cnt(pro.get('zgcpwsw',0)
                                                    +pro.get('rmfygg',0))
                
            elif self.input_distance[node] == 1:
                if self.graph.node[node]['is_human']:
                    
                    # 目标公司董监高&投资人
                    for each_edge in self.graph.out_edges(node,data=True):
                        if self.tar_id == each_edge[1]:
                            if 'DIRECTOR' in each_edge[2]['type']:
                                self.tar_director_node.append(node)
                            elif 'EXECUTIVE' in each_edge[2]['type']:
                                self.tar_executive_node.append(node)                           
                            elif 'SUPERVISOR' in each_edge[2]['type']:
                                self.tar_supervisor_node.append(node)
                            elif 'INVEST' in each_edge[2]['type']:
                                self.tar_invest_inhuman.append(node)
                else:
                    self.get_gs_eb_1_degree_rel_non_ls_num(pro.get('zgcpwsw', 
                                                                   0))
                    self.get_gs_eb_1_deg_lgl_per_rel_jd_num(pro.get('zgcpwsw', 
                                                                    0))
                    self.get_gs_eb_1_degree_lg_pe_re_num_jd(pro.get('zgcpwsw', 
                                                                    0))
                    self.gs_rp_1_degree_rel_not_nainsp.add(pro.get('company_industry',
                                                                   ''))
                    self.get_gs_rp_1_degree_listed_comp_cnt(pro.get('is_ipo',
                                                                    False))
                    self.one_company_node.append(node)
                    
                self.get_gs_rp_1_degree_rel_cnt(node)
                self.gs_rp_1_degree_rel_na_rt.append(pro.get('is_human', False))
                self.get_gs_rp_1_degree_rel_inv_cnt(pro.get('zgcpwsw',0)
                                                    +pro.get('rmfygg',0))
                self.get_gs_rp_1_degree_rel_patent_cnt(pro.get('zhuanli',0))
                
            elif self.input_distance[node] == 0:
                self.tar_node(node)
                
        (gs_rp_leader_pluralism_cnt,
         gs_rp_leader_investment_cnt)= self.get_des_feature()
        (gs_rp_1_degree_rel_comp_di_cnt,
         gs_rp_1_degree_rel_comp_ec) = self.get_company_des_featrure()
        (gs_rp_na_pa_inv_out_cnt,
         gs_rp_na_partner_work_out_cnt) = self.get_investor_inv()
        
        (gs_rp_exe_work_out_cnt,
         gs_rp_exe_investment_out_cnt) = (gs_rp_leader_pluralism_cnt,
                                          gs_rp_leader_investment_cnt)
        
        return {'gs_eb_1_degree_rel_non_ls_num': self.gs_eb_1_degree_rel_non_ls_num,
                'gs_eb_2_degree_rel_non_ls_num': self.gs_eb_2_degree_rel_non_ls_num,
                'gs_eb_2_degree_rel_blackcomp': self.gs_eb_2_degree_rel_blackcomp,
                'gs_eb_2_degree_rel_revoke_num': self.gs_eb_2_degree_rel_revoke_num,
                'gs_eb_2_degree_rel_ltd_abo_bum': self.gs_eb_2_degree_rel_ltd_abo_bum,
                'gs_eb_1_deg_lgl_per_rel_jd_num': self.gs_eb_1_deg_lgl_per_rel_jd_num,
                'gs_eb_1_degree_lg_pe_re_num_jd': self.gs_eb_1_degree_lg_pe_re_num_jd,
                'gs_eb_2_degree_lg_pe_re_jd_num': self.gs_eb_2_degree_lg_pe_re_jd_num,
                'gs_eb_2_degree_rel_exe_num': self.gs_eb_2_degree_rel_exe_num,
                'gs_eb_2_degree_rel_num_exe': self.gs_eb_2_degree_rel_num_exe,
                'gs_eb_threedegree_rel_exe_num': self.gs_eb_threedegree_rel_exe_num,
                'gs_eb_2_degree_lg_pe_re_rv_num': self.gs_eb_2_degree_lg_pe_re_rv_num,
                'gs_rp_1_degree_rel_cnt': self.gs_rp_1_degree_rel_cnt,
                'gs_rp_1_degree_rel_na_rt': self.get_gs_rp_1_degree_rel_na_rt(),
                'gs_rp_1_degree_rel_inv_cnt': self.gs_rp_1_degree_rel_inv_cnt,
                'gs_rp_1_degree_rel_patent_cnt': self.gs_rp_1_degree_rel_patent_cnt,
                'gs_rp_1_degree_rel_not_nainsp': self.get_gs_rp_1_degree_rel_not_nainsp(),
                'gs_rp_2_degree_rel_cnt': self.get_gs_rp_2_degree_rel_cnt(),
                'gs_rp_2_degree_rel_na_rt': self.get_gs_rp_2_degree_rel_na_rt(),
                'gs_rp_2_degree_rel_inv_cnt': self.gs_rp_2_degree_rel_inv_cnt,
                'gs_rp_1_degree_rel_non_nainsp': self.get_gs_rp_1_degree_rel_non_nainsp(),
                'gs_rp_2_degree_rel_patent_cnt': self.gs_rp_2_degree_rel_patent_cnt,
                'gs_rp_leader_pluralism_cnt': gs_rp_leader_pluralism_cnt,
                'gs_rp_leader_investment_cnt': gs_rp_leader_investment_cnt,
                'gs_rp_1_degree_listed_comp_cnt': self.gs_rp_1_degree_listed_comp_cnt,
                'gs_rp_2_degree_rel_na_cnt': self.gs_rp_2_degree_rel_na_cnt,
                'gs_rp_3_degree_rel_na_cnt': self.gs_rp_3_degree_rel_na_cnt,
                'gs_rp_supervisor_cnt': self.get_gs_rp_supervisor_cnt(),
                'gs_rp_1_degree_rel_comp_di_cnt': gs_rp_1_degree_rel_comp_di_cnt,
                'gs_rp_1_degree_rel_comp_ec': gs_rp_1_degree_rel_comp_ec,
                'gs_rp_na_pa_inv_out_cnt': gs_rp_na_pa_inv_out_cnt,
                'gs_rp_na_partner_work_out_cnt': gs_rp_na_partner_work_out_cnt,
                'gs_rp_exe_work_out_cnt': gs_rp_exe_work_out_cnt,
                'gs_rp_exe_investment_out_cnt': gs_rp_exe_investment_out_cnt
                }

            
class DiGraph(object):
    
    def __init__(self, bbd_qyxx_id):
        self.bbd_qyxx_id = bbd_qyxx_id
        (self.company_correlative_nodes, 
         self.company_correlative_edges) = self.get_tid_digraph()

    def init_nx_graph(self, node_list, edge_list, is_muti_graph):
        #网络初始化
        if is_muti_graph:
            DIG = nx.MultiDiGraph()
        else:
            DIG= nx.DiGraph()
        #增加带属性的节点
        #增加带属性的边
        DIG.add_nodes_from(node_list)
        DIG.add_edges_from(edge_list)
        return DIG 
         
    def get_raw_digraph(self):
        with Neo4jDriver.my_driver.session(max_retry_time=3) as session:
            with session.begin_transaction() as tx:
                nodes = tx.run(
                    '''
                    match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-[:INVEST|SUPERVISOR|DIRECTOR|LEGAL|EXECUTIVE*1..4]-(b) 
                    with nodes(p) as np UNWIND np AS x 
                    with DISTINCT x
                    RETURN x
                    ''',
                    bbd_qyxx_id=self.bbd_qyxx_id)
                edges = tx.run(
                    '''
                    match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-[:INVEST|SUPERVISOR|DIRECTOR|LEGAL|EXECUTIVE*1..4]-(b) 
                    with relationships(p) as np UNWIND np AS x 
                    with DISTINCT x
                    RETURN x
                    ''',
                    bbd_qyxx_id=self.bbd_qyxx_id)
                return nodes, edges
                
    def get_tid_digraph(self):
        '''
        从neo4j读取数据
        '''    
        
        def get_node(row):
            row['x'].properties['labels'] = list(each_node['x'].labels)
            if 'Company' in row['x'].properties['labels']:
                label = 'Company'
            elif 'Person' in row['x'].properties['labels']:
                label = 'Person'
            else:
                label = [
                    i
                    for i in row['x'].properties['labels'] 
                    if i != 'Role' and i != 'Entity'][0]
            
            return (row['x'].id, label, row['x'].properties)
    
        def get_edge(row):
            row['x'].properties['type'] = each_edge['x'].type
            return (row['x'].start, row['x'].end)
        
        nodes, edges = self.get_raw_digraph()           
    
        company_correlative_nodes = [
            get_node(each_node)
            for each_node in nodes
        ]
        company_correlative_edges = [
            get_edge(each_edge)
            for each_edge in edges
        ]
        
        return company_correlative_nodes, company_correlative_edges
    
    def __get_prd_edges(self):
        '''
        去掉role节点，返回新的边点三元组
        '''   
        raw_nodes = [
            (node, label) 
            for node, label, pro 
            in self.company_correlative_nodes]
        raw_edges = [(src, des) for src, des in self.company_correlative_edges]
        
        nodes_df = pd.DataFrame(raw_nodes, columns=['node_id', 'label'])
        edge_df = pd.DataFrame(raw_edges, columns=['src', 'des'])
        
        tid_df = edge_df.merge(edge_df, left_on='des', right_on='src')
        prd_df = tid_df.merge(nodes_df, left_on='des_x', right_on='node_id')
        # 以role节点为基准对边进行合并
        prd_df = prd_df[
            (prd_df.label != 'Company')&(prd_df.label != 'Person')
        ].loc[
            :,['src_x', 'des_y', 'label']
        ].sort_values(
            ['src_x', 'des_y']
        )
        prd_df['label'] = prd_df['label'].apply(string.upper)
          
        # 格式化
        prd_edges = map(
            lambda ((src,des),y): (src,des,
                                   {'type': '|'.join(map(itemgetter(2),y))}), 
            groupby(zip(prd_df['src_x'], prd_df['des_y'], prd_df['label']), 
                    key=lambda x: (x[0],x[1])))
        return prd_edges

    
    def get_prd_digraph(self, is_muti_graph=False):
        black_list = self.is_black(filter(None,
                                          [pro.get('bbd_qyxx_id','') 
                                          for node, label, pro 
                                          in self.company_correlative_nodes]))
        def get_pro(pro):
            if pro.get('bbd_qyxx_id', '') in black_list:
                pro[u'is_black'] = True
            else:
                pro[u'is_black'] = False
            return pro
                
        prd_edges = self.__get_prd_edges()
        # 根据角色节点
        tmp_nodes = reduce(lambda x,y:x.union(y), 
                           [set([src,des]) for (src,des,pro) in prd_edges])
        prd_nodes = [
            (node, get_pro(pro))
            for node,label,pro in self.company_correlative_nodes
            if node in tmp_nodes
        ]

        return self.init_nx_graph(prd_nodes, prd_edges, is_muti_graph)
        
    def is_black(self, bbd_qyxx_ids):
        redis_key = 'tmp_wx_node_{}'.format(self.bbd_qyxx_id)
        for each_qyxx_id in bbd_qyxx_ids:        
            redisHelper.pipline.sadd(redis_key, 
                                     each_qyxx_id)
        redisHelper.pipline.execute()
        black_node = redisHelper.client.sinter(redis_key,
                                               'wx_graph_black_set')
        redisHelper.client.delete(redis_key)
        return black_node
     
if __name__ == '__main__':
    #在不考虑网络网络的情况下，创建连接的时间在0.7s左右    
    bbd_qyxx_id = 'ba369113a4c244608fb3541a4a5e6074'  
    #99 152
    with MyTimer(True):
        print '初始化'
        my_graph = DiGraph(bbd_qyxx_id)
    with MyTimer(True):
        print '构造输出子图' 
        prd_graph = my_graph.get_prd_digraph()
        print "处理后的节点个数:" , len(prd_graph)
    with MyTimer(True):    
        feature = RelationFeatureConstruction(prd_graph, bbd_qyxx_id)
        print feature.get_relation_features()
        
        
        