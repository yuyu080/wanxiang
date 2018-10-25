# -*- coding: utf-8 -*-
"""
提交命令：
/opt/spark-2.3.0/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.wanxiang \
cache_to_redis.py

注意:

1、cache指标的计算逻辑更新：
http://git.bbdops.com/jiangsong/bbd-quant-wx-rpc/blob/master/index/graph/compayRelationInfo.py
2、由于缓存的是大型企业的指标结果，这里只计算一度关联方
3、依赖： 
 redis：2.10.3
 networkx: 2.0
 neo4j-driver: 1.5.3
"""


import os
import sys
sys.path.append("/opt/anaconda2c6networkx2/bin/")
import datetime
import string
import math
import json
from itertools import groupby
from operator import itemgetter
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis
import pandas as pd
import numpy as np
import networkx as nx
from neo4j.v1 import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def esdate_delta_time(value):
    try:
        time_format = '%Y-%m-%d'
        dt = datetime.datetime.strptime(value, time_format)
        delta = datetime.datetime.now() - dt
        return delta.days
    except:
        return 0.


class RelationFeatureConstruction(object):
    """
    计算特征的函数集
    """
    
    def __init__(self, graph, bbd_qyxx_id):
        
        if graph:
            self.graph = graph
            for node, attr in graph.nodes(data=True):
                if attr.get('bbd_qyxx_id', '') == bbd_qyxx_id:
                    self.tar_id = node
                    self.input_distance = nx.shortest_path_length(
                        self.graph.to_undirected(), source=self.tar_id)
        else:
            pass
        
        # 目标公司董监高列表
        self.tar_director_node = []
        self.tar_executive_node = []
        self.tar_supervisor_node = []
        
        # 目标公司自然人股东
        self.tar_invest_human = []
        
        # 关联方企业
        self.one_company_node = []
        self.two_company_node = []
        self.three_company_node = []
        
        # 自然人节点
        self.one_human_node = []
        self.two_human_node = []
        self.three_human_node = []
        
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
        self.gs_rp_2_degree_rel_non_nainsp = set()
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
        try:
            count = Counter(self.gs_rp_1_degree_rel_na_rt)
            return round(count[True]*1. / (count[True]+count[False]), 3)
        except:
            return 0.
    
    def get_gs_rp_1_degree_rel_inv_cnt(self, node_attr):
        self.gs_rp_1_degree_rel_inv_cnt += node_attr
        
    def get_gs_rp_1_degree_rel_patent_cnt(self, node_attr):
        self.gs_rp_1_degree_rel_patent_cnt += node_attr
    
    def get_gs_rp_1_degree_rel_not_nainsp(self):
        return len(self.gs_rp_1_degree_rel_not_nainsp)
        
    def get_gs_rp_2_degree_rel_cnt(self):
        return len(self.gs_rp_2_degree_rel_cnt)
    
    def get_gs_rp_2_degree_rel_na_rt(self):
        try:
            count = Counter(self.gs_rp_2_degree_rel_na_rt)
            return round(count[True]*1. / (count[True]+count[False]), 3)
        except:
            return 0.
    
    def get_gs_rp_2_degree_rel_inv_cnt(self, node_attr):
        self.gs_rp_2_degree_rel_inv_cnt += node_attr
    
    def get_gs_rp_1_degree_rel_non_nainsp(self):
        return len(self.gs_rp_1_degree_rel_non_nainsp)
        
    def get_gs_rp_2_degree_rel_non_nainsp(self):
        return len(self.gs_rp_2_degree_rel_non_nainsp)
    
    def get_gs_rp_2_degree_rel_patent_cnt(self, node_attr):
        self.gs_rp_2_degree_rel_patent_cnt += node_attr
    
    def get_des_feature(self):
        """
        目标公司董监高对外投资&兼职
        """
        company_des_node = set()
        company_inv_node = set()
        for each_node in (self.tar_director_node+
                          self.tar_executive_node+
                          self.tar_supervisor_node):
            for each_edge in self.graph.out_edges(each_node, data=True):
                if (('DIRECTOR' in each_edge[2]['type'] or
                        'EXECUTIVE' in each_edge[2]['type'] or
                        'SUPERVISOR' in each_edge[2]['type']) and self.tar_id != each_edge[1]):
                    company_des_node.add(each_edge[1])
                if ('INVEST' in each_edge[2]['type'] and 
                        self.tar_id != each_edge[1]):
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
        """
        一度公司董事，高管数
        """
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
        """
        目标公司自然人股东对外投资&在外任职数量
        """
        out_invest_degree = 0
        out_des_degree = 0
        for each_node in self.tar_invest_human:
            for each_edge in self.graph.out_edges(each_node, data=True):
                if 'INVEST' in each_edge[2]['type'] and self.tar_id != each_edge[1]:
                    out_invest_degree += 1
                if ('DIRECTOR' in each_edge[2]['type'] or
                        'EXECUTIVE' in each_edge[2]['type'] or
                        'SUPERVISOR' in each_edge[2]['type']) and self.tar_id != each_edge[1]:
                    out_des_degree += 1
        return out_invest_degree, out_des_degree

    def get_gs_rp_net_cluster_coefficient(self):
        try:
            return round(nx.cluster.average_clustering(
                            self.graph.to_undirected()),2)
        except:
            return 0
    
    def get_gs_rp_core_na_one_ctr_node_cnt(self):
        try:
            max_control = max(
                num for node,num in self.graph.out_degree(self.one_human_node))
            return max_control
        except:
            return 0

    def get_qy_hxzrr_kzjd_num(self):
        try:
            max_three_control = sorted(
                [num for node,num in self.graph.out_degree(self.one_human_node)], reverse=True
            )[:3]
            return sum(max_three_control)
        except:
            return 0

    def get_gs_rp_1_wn_3_degree_rel_ncmcc(self):
        try:
            max_control = max(
                num for node,num in self.graph.out_degree(self.one_human_node +
                                                          self.two_human_node +
                                                          self.three_human_node))        
            return max_control
        except:
            return 0
        
    def get_gs_rp_1_wn_3_degree_rel_lpcmcc(self):
        try:
            max_control = max(
                num for node,num in self.graph.out_degree(self.one_company_node +
                                                          self.two_company_node +
                                                          self.three_company_node))
            return max_control
        except:
            return 0
            
    def get_gs_rp_2_degree_rel_lg_pe_cnt(self):
        return len(self.two_company_node)
    
    def get_gs_rp_invest_out_comp_cnt(self):
        return self.graph.out_degree(self.tar_id)
    
    def get_gs_rp_legal_rel_cnt(self):
        return len(self.one_company_node +
                   self.two_company_node +
                   self.three_company_node)
    
    def get_gs_eo_lagal_person_sh_ext_time(self):
        """
        法人股东平均存续时间
        """
        invest_company = set()
        for each_node in self.one_company_node:
            for each_edge in self.graph.out_edges(each_node, data=True):
                if self.tar_id == each_edge[1]:
                    invest_company.add(each_node)
        invest_company_estime = [
            esdate_delta_time(self.graph.nodes[node].get('esdate', ''))
            for node in invest_company
        ]
        
        try:
            return sum(invest_company_estime) / len(invest_company_estime)
        except:
            return 0.
    
    def get_gs_eo_3_degree_lg_pr_rel_ext_t(self):
        invest_company = set()
        for each_node in (self.one_company_node+
                          self.two_company_node+
                          self.three_company_node):
            invest_company.add(each_node)
        invest_company_estime = [
            esdate_delta_time(self.graph.nodes[node].get('esdate', ''))
            for node in invest_company
        ]
        
        try:
            return round(sum(invest_company_estime) / 
                         len(invest_company_estime), 3)
        except:
            return 0.         

    def get_exe_abnormal_business_risk(self):
        """
        被执行风险，经营异常风险
        """
        dishonesty_num = [0]*4
        zhixing_num = [0]*4
        jyyc_num = [0]*4
        estatus_num = [0]*4
        for node, pro in self.graph.node(data=True):
            if self.input_distance[node] == 3:
                dishonesty_num[3] += pro.get('dishonesty', 0)
                zhixing_num[3] += pro.get('zhixing', 0)
                jyyc_num[3] += pro.get('jyyc', 0)
                estatus_num[3] += 1 if (u'吊销' in pro.get('estatus', '') or
                                        u'注销' in pro.get('estatus', '')) else 0
            elif self.input_distance[node] == 2:
                dishonesty_num[2] += pro.get('dishonesty', 0)
                zhixing_num[2] += pro.get('zhixing', 0)
                jyyc_num[2] += pro.get('jyyc', 0)
                estatus_num[2] += 1 if (u'吊销' in pro.get('estatus', '') or
                                        u'注销' in pro.get('estatus', '')) else 0
            elif self.input_distance[node] == 1:
                dishonesty_num[1] += pro.get('dishonesty', 0)
                zhixing_num[1] += pro.get('zhixing', 0)
                jyyc_num[1] += pro.get('jyyc', 0)
                estatus_num[1] += 1 if (u'吊销' in pro.get('estatus', '') or
                                        u'注销' in pro.get('estatus', '')) else 0
            else:
                dishonesty_num[0] += pro.get('dishonesty', 0)
                zhixing_num[0] += pro.get('zhixing', 0)
                jyyc_num[0] += pro.get('jyyc', 0)
                estatus_num[0] += 1 if (u'吊销' in pro.get('estatus', '') or
                                        u'注销' in pro.get('estatus', '')) else 0
                
        risk1 = np.dot(
            map(lambda (x, y): x+2*y, zip(zhixing_num, dishonesty_num)),
            [1., 1/2., 1/3., 1/4.]
        )
        
        risk2 = np.dot(
            map(lambda (x, y): x+2*y, zip(jyyc_num, estatus_num)),
            [1., 1/2., 1/3., 1/4.]
        )
        
        return round(float(risk1), 3), round(float(risk2), 3)

    def get_gs_rp_actual_ctr_risk(self):
        """
        实际控制人风险
        """
        def get_degree_distribution(is_human):
            if is_human:
                sets = (self.one_human_node+
                        self.two_human_node+
                        self.three_human_node)
            else:
                sets = (self.one_company_node+
                        self.two_company_node+
                        self.three_company_node)
            person_out_degree = [v for k,v in self.graph.out_degree(sets)]
            if not person_out_degree:
                person_out_degree.append(0)
            return person_out_degree
        
        nature_person_distribution = get_degree_distribution(1)
        legal_person_distribution = get_degree_distribution(0)
        
        nature_max_control = max(nature_person_distribution)
        legal_max_control = max(legal_person_distribution)
        nature_avg_control = round(np.average(nature_person_distribution), 2)
        legal_avg_control = round(np.average(legal_person_distribution), 2)
        
        total_legal_num = len(self.one_company_node+
                              self.two_company_node+
                              self.three_company_node)
        
        risk = round(((
                    2*(nature_max_control + legal_max_control) + 
                    (nature_avg_control + legal_avg_control)) /
                (2*total_legal_num + 0.001)), 2)
        
        return risk
        
    def get_gs_rp_comp_expend_path_risk(self):
        """
        公司扩张路径风险
        """
        nature_person_distribution = {
            1: len(self.one_human_node),
            2: len(self.two_human_node),
            3: len(self.three_human_node)
        }
        legal_person_distribution = {
            1: len(self.one_company_node),
            2: len(self.two_company_node),
            3: len(self.three_company_node)
        }

        nature_person_num = [
            nature_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 4)]
        legal_person_num = [
            legal_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 4)]

        risk = round(np.sum(
                np.divide(
                    [
                        np.divide(
                            np.sum(nature_person_num[:each_distance]), 
                            np.sum(legal_person_num[:each_distance]),
                            dtype=float)
                        for each_distance in range(1, 4)], 
                    np.array([1, 2, 3], dtype=float))), 2)

        return risk if not math.isinf(risk) else 0
        
    def get_gs_rp_rel_center_cluster_risk(self):
        """
        关联方中心集聚风险
        """
        legal_person_shareholder = set()
        legal_person_subsidiary = set()
        for each_node in self.one_company_node:
            for each_edge in self.graph.out_edges(each_node, data=True):
                if self.tar_id == each_edge[1]:
                    legal_person_shareholder.add(each_node)
            for each_edge in self.graph.in_edges(each_node, data=True):
                if self.tar_id == each_edge[0]:
                    legal_person_subsidiary.add(each_node)
        risk = len(legal_person_subsidiary) - len(legal_person_shareholder)
        
        return risk
        
    def get_gs_rp_rel_strt_stability_risk(self):
        """
        关联方结构稳定风险
        """
        relation_three_num = len(self.three_human_node)+len(self.three_company_node)
        relation_two_num = len(self.two_human_node)+len(self.two_company_node)
        relation_one_num = len(self.one_human_node)+len(self.one_company_node)
        relation_zero_num = 1
        
        x = np.array([
                relation_zero_num, 
                relation_one_num, 
                relation_two_num, 
                relation_three_num]).astype(float)
        
        y_2 = x[2] / (x[1]+x[2])
        y_3 = x[3] / (x[1]+x[2]+x[3])
        risk = y_2/2 + y_3/3

        return round(risk, 3)
        
    def get_isSOcompany(self):
        """
        国企&非国企法人股东数
        """
        qy_gqfrgd_Num = 0
        qy_fgqfrgd_Num = 0

        for each_node in self.one_company_node:
            for each_edge in self.graph.out_edges(each_node, data=True):
                if self.tar_id == each_edge[1] and 'INVEST' in each_edge[2]['type']:
                    if self.graph.node[each_node]['isSOcompany']:
                        qy_gqfrgd_Num += 1
                    else:
                        qy_fgqfrgd_Num += 1

        return qy_gqfrgd_Num, qy_fgqfrgd_Num

    def get_qy_gd_ssfr_num(self):
        """
        上市法人股东数
        """
        qy_gd_ssfr_num = 0

        for each_node in self.one_company_node:
            for each_edge in self.graph.out_edges(each_node, data=True):
                if self.tar_id == each_edge[1] and 'INVEST' in each_edge[2]['type']:
                    if self.graph.node[each_node]['is_ipo']:
                        qy_gd_ssfr_num += 1
        return qy_gd_ssfr_num

    def get_relation_feature(self, nodes):
        """
        一些关联方指标
        """
        resutl = dict(ktgg=0,
                      rmfygg=0,
                      zgcpwsw=0,
                      xzcf=0,
                      zhixing=0,
                      jyyc=0,
                      dishonesty=0,
                      estatus=0,
                      has_ktgg=0,
                      SOcompany=0,
                      ipocompany=0)

        for each_node in nodes:
            resutl['ktgg'] += self.graph.node[each_node].get('ktgg', 0)
            resutl['rmfygg'] += self.graph.node[each_node].get('rmfygg', 0)
            resutl['zgcpwsw'] += self.graph.node[each_node].get('zgcpwsw', 0)
            resutl['xzcf'] += self.graph.node[each_node].get('xzcf', 0)
            resutl['zhixing'] += self.graph.node[each_node].get('zhixing', 0)
            resutl['dishonesty'] += self.graph.node[each_node].get('dishonesty', 0)
            resutl['jyyc'] += self.graph.node[each_node].get('jyyc', 0)
            resutl['estatus'] += 1 if u'吊销' in self.graph.node[each_node].get('estatus', '') else 0

            resutl['has_ktgg'] += 1 if self.graph.node[each_node].get('ktgg', 0) else 0
            resutl['SOcompany'] += 1 if self.graph.node[each_node].get('isSOcompany', False) else 0
            resutl['ipocompany'] += 1 if self.graph.node[each_node].get('is_ipo', False) else 0

        return resutl

    def get_out_degree(self):
        """
        实际控制人
        """
        def get_degree_distribution(is_human):
            some_person_sets = [
                node
                for node, attr in self.graph.node(data=True)
                if attr['is_human'] == is_human]
            person_out_degree = dict(self.graph.out_degree(some_person_sets)).values()
            if not person_out_degree:
                person_out_degree.append(0)
            return person_out_degree

        nature_person_distribution = get_degree_distribution(1)
        legal_person_distribution = get_degree_distribution(0)

        nature_max_control = max(nature_person_distribution)
        legal_max_control = max(legal_person_distribution)
        nature_avg_control = round(np.average(nature_person_distribution), 2)
        legal_avg_control = round(np.average(legal_person_distribution), 2)

        return nature_max_control, legal_max_control, nature_avg_control, legal_avg_control

    def get_invest_info(self):
        qy_frgd_Num = len([
                src_node
                for src_node, des_node, edge_attr
                in self.graph.edges(self.one_company_node, data=True)
                if des_node == self.tar_id
                and 'INVEST' in edge_attr['type']
                and self.graph.node[src_node]['is_human'] == 0])
        qy_dwtzgs_Num = len([
                des_node
                for src_node, des_node, edge_attr
                in self.graph.edges(self.tar_id, data=True)
                if src_node == self.tar_id
                and 'INVEST' in edge_attr['type']
                and self.graph.node[des_node]['is_human'] == 0])
        return qy_dwtzgs_Num, qy_frgd_Num

    def get_same_address(self):
        """
        相同地址
        """
        legal_person_address = [
            attr.get('address', '')
            for node, attr in self.graph.node(data=True)
            if not attr['is_human']]
        c = Counter(
            filter(
                lambda x: x is not None and len(x) >= 21,
                legal_person_address))
        n = c.most_common(1)
        n = n[0][1] if len(n) > 0 else 1
        return n

    def get_short_risk(self):
        """
        短期逐利风险
        """

        def get_max_established(distance, timedelta):
            """
            获取在某distance关联方企业中，某企业在任意timedelta内投资成立公司数量的最大值
            """

            def get_date(date):
                try:
                    return datetime.datetime.strptime(date, '%Y-%m-%d')
                except:
                    return 0

            # 用于获取最大连续时间数，这里i的取值要仔细琢磨一下，这里输入一个时间差序列与时间差
            def get_date_density(difference_list, timedelta):
                time_density_list = []
                for index, date in enumerate(difference_list):
                    if date < timedelta:
                        s = 0
                        i = 1
                        while s < timedelta and i <= len(difference_list) - index:
                            i += 1
                            s = sum(difference_list[index:index+i])
                        time_density_list.append(i)
                    else:
                        continue
                return max(time_density_list) if len(time_density_list) > 0 else 0

            # distance所有节点集合
            default_set = {
                0: [self.tar_id],
                1: self.one_company_node+self.one_human_node,
                2: self.two_company_node+self.two_human_node,
                3: self.three_company_node+self.three_human_node
            }
            relation_set = default_set[distance]
            investment_dict = defaultdict(lambda: [])

            if len(self.graph.edges) > 1:
                for src_node, des_node, edge_attr in (
                        self.graph.edges(relation_set, data=True)):
                    if ('INVEST' in edge_attr['type']
                            and self.graph.node[des_node].get('esdate', 0) != '-'
                            and not self.graph.node[src_node]['is_human']):
                        # 将所有节点投资的企业的成立时间加进列表中
                        investment_dict[src_node].append(
                            get_date(self.graph.node[des_node].get('esdate', 0))
                        )

            # 目标企业所有节点所投资的企业时间密度字典
            all_date_density_dict = {}

            for node, date_list in investment_dict.iteritems():
                # 构建按照时间先后排序的序列
                date_strp_list = sorted(date_list)
                if len(date_strp_list) > 1:
                    # 构建时间差的序列，例如：[256, 4, 5, 1, 2, 33, 6, 5, 4, 73]
                    date_difference_list = [
                        (date_strp_list[index + 1] - date_strp_list[index]).days
                        for index in range(len(date_strp_list) - 1)]
                    # 计算某法人节点在timedelta天之内有多少家公司成立
                    es_num = get_date_density(date_difference_list, timedelta)
                    if es_num in all_date_density_dict:
                        all_date_density_dict[es_num].append(node)
                    else:
                        all_date_density_dict[es_num] = [node]
                else:
                    continue
            keys = all_date_density_dict.keys()
            max_num = max(keys) if len(keys) > 0 else 0

            return max_num

        x = [
            get_max_established(each_distance, 180)
            for each_distance in xrange(0, 4)]

        return x

    def get_relation_features(self):
        """
        计算入口
        """
        for node, pro in self.graph.node(data=True):
            if self.input_distance[node] == 3:
                if self.graph.node[node]['is_human']:
                    self.get_gs_rp_3_degree_rel_na_cnt(node)
                    self.three_human_node.append(node)
                else:
                    self.get_gs_eb_threedegree_rel_exe_num(pro.get('zhixing', 
                                                                   0))
                    self.three_company_node.append(node)
            elif self.input_distance[node] == 2:
                if self.graph.node[node]['is_human']:
                    self.get_gs_rp_2_degree_rel_na_cnt(node)
                    self.two_human_node.append(node)
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
                    self.gs_rp_2_degree_rel_non_nainsp.add(pro.get('company_industry',
                                                                   ''))
                    self.get_gs_eb_2_degree_lg_pe_re_rv_num(pro.get('estatus',
                                                                    ''))
                    self.get_gs_rp_2_degree_rel_patent_cnt(pro.get('zhuanli',
                                                                   0))
                    self.two_company_node.append(node)
                    
                self.gs_rp_2_degree_rel_cnt.append(node)
                self.gs_rp_2_degree_rel_na_rt.append(pro.get('is_human', False))
                self.get_gs_rp_2_degree_rel_inv_cnt(pro.get('zgcpwsw', 0)
                                                    + pro.get('rmfygg', 0))
                
            elif self.input_distance[node] == 1:
                if self.graph.node[node]['is_human']:
                    self.one_human_node.append(node)
                    
                    # 目标公司董监高&投资人
                    for each_edge in self.graph.out_edges(node,data=True):
                        if self.tar_id == each_edge[1]:
                            if 'DIRECTOR' in each_edge[2]['type']:
                                self.tar_director_node.append(node)
                            if 'EXECUTIVE' in each_edge[2]['type']:
                                self.tar_executive_node.append(node)                           
                            if 'SUPERVISOR' in each_edge[2]['type']:
                                self.tar_supervisor_node.append(node)
                            if 'INVEST' in each_edge[2]['type']:
                                self.tar_invest_human.append(node)
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
                    self.gs_rp_1_degree_rel_non_nainsp.add(pro.get('company_industry',
                                                                   ''))
                    
                self.get_gs_rp_1_degree_rel_cnt(node)
                self.gs_rp_1_degree_rel_na_rt.append(pro.get('is_human', False))
                self.get_gs_rp_1_degree_rel_inv_cnt(pro.get('zgcpwsw', 0)
                                                    + pro.get('rmfygg', 0))
                self.get_gs_rp_1_degree_rel_patent_cnt(pro.get('zhuanli', 0))
                
            elif self.input_distance[node] == 0:
                pass
                
        (gs_rp_leader_pluralism_cnt,
         gs_rp_leader_investment_cnt) = self.get_des_feature()
        (gs_rp_1_degree_rel_comp_di_cnt,
         gs_rp_1_degree_rel_comp_ec) = self.get_company_des_featrure()
        (gs_rp_na_pa_inv_out_cnt,
         gs_rp_na_partner_work_out_cnt) = self.get_investor_inv()
        
        (gs_rp_exe_work_out_cnt,
         gs_rp_exe_investment_out_cnt) = (gs_rp_leader_pluralism_cnt,
                                          gs_rp_leader_investment_cnt)
        
        (gs_eb_exe_risk,
         gs_eb_abnormal_business_risk) = self.get_exe_abnormal_business_risk()

        (qy_gqfrgd_Num, qy_fgqfrgd_Num) = self.get_isSOcompany()

        one_relation_features = self.get_relation_feature(self.one_company_node)
        two_relation_features = self.get_relation_feature(self.two_company_node)
        three_relation_features = self.get_relation_feature(self.three_company_node)

        (qy_3dzrrzdkzqy_Max, deg3_frzdkzqy_Max,
         deg3_zrrzdkzqy_Avg, deg3_glfrkzqy_Avg) = self.get_out_degree()

        qy_dwtzgs_Num, qy_frgd_Num = self.get_invest_info()

        (qy_6yncltzgs_Max, deg1_6ynclgs_Max,
         deg2_6ynclgs_Max, deg3_6ynclgs_Max) = self.get_short_risk()

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
                'gs_rp_2_degree_rel_non_nainsp': self.get_gs_rp_2_degree_rel_non_nainsp(),
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
                'gs_rp_exe_investment_out_cnt': gs_rp_exe_investment_out_cnt,
                'gs_rp_net_cluster_coefficient': self.get_gs_rp_net_cluster_coefficient(),
                'gs_rp_core_na_one_ctr_node_cnt': self.get_gs_rp_core_na_one_ctr_node_cnt(),
                'gs_rp_1_wn_3_degree_rel_ncmcc': self.get_gs_rp_1_wn_3_degree_rel_ncmcc(),
                'gs_rp_1_wn_3_degree_rel_lpcmcc': self.get_gs_rp_1_wn_3_degree_rel_lpcmcc(),
                'gs_rp_2_degree_rel_lg_pe_cnt': self.get_gs_rp_2_degree_rel_lg_pe_cnt(),
                'gs_rp_invest_out_comp_cnt': self.get_gs_rp_invest_out_comp_cnt(),
                'gs_rp_legal_rel_cnt': self.get_gs_rp_legal_rel_cnt(),
                'gs_eo_lagal_person_sh_ext_time': self.get_gs_eo_lagal_person_sh_ext_time(),
                'gs_eo_3_degree_lg_pr_rel_ext_t': self.get_gs_eo_3_degree_lg_pr_rel_ext_t(),
                'gs_eb_exe_risk': gs_eb_exe_risk,
                'gs_eb_abnormal_business_risk': gs_eb_abnormal_business_risk,
                'gs_rp_actual_ctr_risk': self.get_gs_rp_actual_ctr_risk(),
                'gs_rp_comp_expend_path_risk': self.get_gs_rp_comp_expend_path_risk(),
                'gs_rp_rel_center_cluster_risk': self.get_gs_rp_rel_center_cluster_risk(),
                'gs_rp_rel_strt_stability_risk': self.get_gs_rp_rel_strt_stability_risk(),
                'qy_gqfrgd_Num': qy_gqfrgd_Num,
                'qy_fgqfrgd_Num': qy_fgqfrgd_Num,
                'qy_zrrgd_Num': len(self.tar_invest_human),
                'deg1_ktgg_Num': one_relation_features['ktgg'],
                'deg1_fygg_Num': one_relation_features['rmfygg'],
                'deg1_cpws_Num': one_relation_features['zgcpwsw'],
                'deg1_xzcf_Num': one_relation_features['xzcf'],
                'deg1_bzx_Num': one_relation_features['zhixing'],
                'deg1_sxbzx_Num': one_relation_features['dishonesty'],
                'deg1_frdxxx_Num': one_relation_features['estatus'],
                'deg1_frjyycxx_Num': one_relation_features['jyyc'],
                'deg2_ktgg_Num': two_relation_features['ktgg'],
                'deg2_fygg_Num': two_relation_features['rmfygg'],
                'deg2_cpws_Num': two_relation_features['zgcpwsw'],
                'deg2_xzcf_Num': two_relation_features['xzcf'],
                'deg2_bzx_Num': two_relation_features['zhixing'],
                'deg2_sxbzx_Num': two_relation_features['dishonesty'],
                'deg2_frdxxx_Num': two_relation_features['estatus'],
                'deg2_frjyycxx_Num': two_relation_features['jyyc'],
                'deg3_ktgg_Num': three_relation_features['ktgg'],
                'deg3_fygg_Num': three_relation_features['rmfygg'],
                'deg3_cpws_Num': three_relation_features['zgcpwsw'],
                'deg3_xzcf_Num': three_relation_features['xzcf'],
                'deg3_bzx_Num': three_relation_features['zhixing'],
                'deg3_sxbzx_Num': three_relation_features['dishonesty'],
                'deg3_frdxxx_Num': three_relation_features['estatus'],
                'deg3_frjyycxx_Num': three_relation_features['jyyc'],
                'qy_3dzrrzdkzqy_Max': qy_3dzrrzdkzqy_Max,
                'deg3_frzdkzqy_Max': deg3_frzdkzqy_Max,
                'deg3_zrrzdkzqy_Avg': deg3_zrrzdkzqy_Avg,
                'deg3_glfrkzqy_Avg': deg3_glfrkzqy_Avg,
                'deg1_fr_Num': len(self.one_company_node),
                'deg2_fr_Num': len(self.two_company_node),
                'deg3_fr_Num': len(self.three_company_node),
                'deg1_zzr_Num': len(self.one_human_node),
                'deg2_zzr_Num': len(self.two_human_node),
                'deg3_zzr_Num': len(self.three_human_node),
                'qy_dwtzgs_Num': qy_dwtzgs_Num,
                'qy_frgd_Num': qy_frgd_Num,
                'deg1_glf_Num': len(self.one_company_node)+len(self.one_human_node),
                'deg2_glf_Num': len(self.two_company_node)+len(self.two_human_node),
                'deg3_glf_Num': len(self.three_company_node)+len(self.three_human_node),
                'deg3_glqydzxt_Max': self.get_same_address(),
                'fr_glf_Num': len(self.one_company_node)+len(self.two_company_node)+len(self.three_company_node),
                'qy_6yncltzgs_Max': qy_6yncltzgs_Max,
                'deg1_6ynclgs_Max': deg1_6ynclgs_Max,
                'deg2_6ynclgs_Max': deg2_6ynclgs_Max,
                'deg3_6ynclgs_Max': deg3_6ynclgs_Max,
                'qy_hxzrr_kzjd_num': self.get_qy_hxzrr_kzjd_num(),
                'qy_gd_ssfr_num': self.get_qy_gd_ssfr_num(),
                'deg1_fr_czktgg_num': one_relation_features['has_ktgg'],
                'deg2_fr_czktgg_num': two_relation_features['has_ktgg'],
                'deg3_fr_czktgg_num': three_relation_features['has_ktgg'],
                'deg1_gqfr_num': one_relation_features['SOcompany'],
                'deg2_gqfr_num': two_relation_features['SOcompany'],
                'deg3_gqfr_num': three_relation_features['SOcompany'],
                'deg1_ssfr_num': one_relation_features['ipocompany'],
                'deg2_ssfr_num': two_relation_features['ipocompany'],
                'deg3_ssfr_num': three_relation_features['ipocompany']
                }
            

def get_raw_digraph(driver, bbd_qyxx_id):
    with driver.session(max_retry_time=1) as session:
        with session.begin_transaction() as tx:
            nodes = tx.run(
                                '''
                                match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-
                                [:INVEST|SUPERVISOR|DIRECTOR|LEGAL|EXECUTIVE*0..2]-(b) 
                                with nodes(p) as np UNWIND np AS x 
                                with DISTINCT x
                                RETURN x
                                ''',
                                {'bbd_qyxx_id': bbd_qyxx_id})
    
    with driver.session(max_retry_time=1) as session:
        with session.begin_transaction() as tx:
            edges = tx.run(
                                '''
                                match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-
                                [:INVEST|SUPERVISOR|DIRECTOR|LEGAL|EXECUTIVE*0..2]-(b) 
                                with relationships(p) as np UNWIND np AS x 
                                with DISTINCT x
                                RETURN x
                                ''',
                                {'bbd_qyxx_id': bbd_qyxx_id})

    return nodes, edges


def get_tid_digraph(driver, bbd_qyxx_id):
    """
    从neo4j读取数据
    """
    
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
        
        return row['x'].id, label, row['x'].properties

    def get_edge(row):
        row['x'].properties['type'] = each_edge['x'].type
        return row['x'].start, row['x'].end
    
    nodes, edges = get_raw_digraph(driver, bbd_qyxx_id)           

    company_correlative_nodes = [
        get_node(each_node)
        for each_node in nodes
    ]
    company_correlative_edges = [
        get_edge(each_edge)
        for each_edge in edges
    ]
    
    return company_correlative_nodes, company_correlative_edges


def __get_prd_edges(company_correlative_nodes, company_correlative_edges):
    """
    去掉role节点，返回新的边点三元组
    """
    raw_nodes = [
        (node, label) 
        for node, label, pro 
        in company_correlative_nodes]
    raw_edges = [(src, des) for src, des in company_correlative_edges]
    
    nodes_df = pd.DataFrame(raw_nodes, columns=['node_id', 'label'])
    edge_df = pd.DataFrame(raw_edges, columns=['src', 'des'])
    
    tid_df = edge_df.merge(edge_df, left_on='des', right_on='src')
    prd_df = tid_df.merge(nodes_df, left_on='des_x', right_on='node_id')
    # 以role节点为基准对边进行合并
    prd_df = prd_df[
        (prd_df.label != 'Company') & (prd_df.label != 'Person')
    ].loc[
        :, ['src_x', 'des_y', 'label']
    ].sort_values(
        ['src_x', 'des_y']
    )
    prd_df['label'] = prd_df['label'].apply(string.upper)
      
    # 格式化
    prd_edges = map(
        lambda ((src,des), y): (src, des,
                               {'type': '|'.join(map(itemgetter(2), y))}),
        groupby(zip(prd_df['src_x'], prd_df['des_y'], prd_df['label']), 
                key=lambda x: (x[0], x[1])))
    return prd_edges


def init_nx_graph(node_list, edge_list, is_muti_graph):
    # 网络初始化
    if is_muti_graph:
        DIG = nx.MultiDiGraph()
    else:
        DIG = nx.DiGraph()
    # 增加带属性的节点
    # 增加带属性的边
    DIG.add_nodes_from(node_list)
    DIG.add_edges_from(edge_list)
    return DIG    
 
    
def is_black(bbd_qyxx_ids, bbd_qyxx_id, pipline, client):
    redis_key = 'tmp_wx_node_{}'.format(bbd_qyxx_id)
    for each_qyxx_id in bbd_qyxx_ids:        
        pipline.sadd(redis_key, 
                          each_qyxx_id)
    pipline.execute()
    black_node = client.sinter(redis_key,
                               'wx_graph_black_set')
    client.delete(redis_key)
    return black_node
    
    
def get_prd_digraph(company_correlative_nodes, bbd_qyxx_id, pipline, 
                    client, prd_edges, is_muti_graph=False):
    black_list = is_black(filter(None,
                                 [pro.get('bbd_qyxx_id', '') for node, label, pro in company_correlative_nodes]),
                          bbd_qyxx_id, 
                          pipline, 
                          client
                          )

    def get_pro(pro):
        if pro.get('bbd_qyxx_id', '') in black_list:
            pro[u'is_black'] = True
        else:
            pro[u'is_black'] = False
        return pro
    
    if prd_edges:
        # 根据角色节点
        tmp_nodes = reduce(lambda x, y: x.union(y),
                           [{src, des} for (src, des, pro) in prd_edges])
        prd_nodes = [
            (node, get_pro(pro))
            for node, label, pro in company_correlative_nodes
            if node in tmp_nodes
        ]
    else:
        prd_nodes = [
            (node, get_pro(pro))
            for node, label, pro in company_correlative_nodes
        ]

    return init_nx_graph(prd_nodes, prd_edges, is_muti_graph)
    

def get_each_comapny_info(rows):
    driver = GraphDatabase.driver(NEO4J_URL, 
                                  auth=(NEO4J_USER, NEO4J_PASSWORD))
    pool = redis.ConnectionPool(host=REDIS_URL_ONE, port=REDIS_PORT_ONE, 
                                password=REDIS_PASSWORD_ONE, db=0)
    client = redis.Redis(connection_pool=pool)
    pipline = client.pipeline(transaction=True)
    
    result = []
    for bbd_qyxx_id in rows:
        try:
            (company_correlative_nodes, 
             company_correlative_edges) = get_tid_digraph(driver, bbd_qyxx_id)
            prd_edges = __get_prd_edges(company_correlative_nodes, 
                                        company_correlative_edges)
            prd_graph = get_prd_digraph(company_correlative_nodes, bbd_qyxx_id, 
                                        pipline, client, prd_edges)
            
            relation_feature = RelationFeatureConstruction(prd_graph, bbd_qyxx_id)
            result.append((bbd_qyxx_id, relation_feature.get_relation_features()))
        except Exception as e:
            pass
        
    return result
    
    
def thread(fun, data):
    results = []
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(fun, each_data) for each_data in data]
        try:
            for future in as_completed(futures, timeout=10):
                results.append(future.result())
        except Exception as e:
            pass
        
    return results    
    
    
def to_redis(iterator):

    def to_each_server(url, port, password):
        pool = redis.ConnectionPool(host=url, port=port, 
                                    password=password, db=0)
        client = redis.Redis(connection_pool=pool)
        pipline = client.pipeline(transaction=True)
        
        for each_data in iterator:
            pipline.set('quant_wx_index:companyRelationInfo:{}'.format(each_data[0]), json.dumps(each_data[1]))
        pipline.execute()

    # 需要同时写多个redis
    to_each_server(REDIS_URL_ONE, REDIS_PORT_ONE, REDIS_PASSWORD_ONE)
    to_each_server(REDIS_URL_TWO, REDIS_PORT_TWO, REDIS_PASSWORD_TWO)
    to_each_server(REDIS_URL_THREE, REDIS_PORT_THREE, REDIS_PASSWORD_THREE)
    

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "15g")
    conf.set("spark.executor.instances", 20)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)

    spark = SparkSession \
        .builder \
        .appName("wanxiang_region_node_and_edge") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 


def run():
    sample = spark.read.json(
        '{path}/{file_name}'.format(path=IN_PATH, 
                                    file_name=FILE_NAME)
    ).select('a')
        
    test2 = spark.sparkContext.parallelize(sample.rdd.take(3))

    sample.rdd.repartition(5000).mapPartitions(
        lambda rows: thread(get_each_comapny_info,  rows)
    ).flatMap(
        lambda x: x
    ).foreachPartition(
        to_redis
    )


if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = '/opt/anaconda2c6networkx2/bin/python'
    
    # 输入参数
    SAMPLE_PATH = '/user/wanxiang/inputdata/'
    FILE_NAME = 'company_with_big_relation_20180706'
    IN_PATH = '/user/wanxiang/inputdata/'
    TMP_PATH = '/user/wanxiang/tmpdata/'
    OUT_PATH = '/user/wanxiang/step_five/'
    
    # 数据库参数
    NEO4J_URL = 'bolt://neo4j.prod.bbdops.com:27687'
    NEO4J_USER = 'wanxiangreader'
    NEO4J_PASSWORD = '087e983d822bf8f2ee029a14982b903b'
    
    # 4G redis 3.2-HA
    REDIS_URL_ONE = '10.28.70.11'
    REDIS_PORT_ONE = 6392
    REDIS_PASSWORD_ONE = 'dhksjf9peuf32d2l'
    
    # 4G redis 3.4-5core
    REDIS_URL_TWO = '10.28.60.15'
    REDIS_PORT_TWO = 26382
    REDIS_PASSWORD_TWO = 'wanxiang'    
    
    # 2G redis grey
    REDIS_URL_THREE = '10.28.70.11'
    REDIS_PORT_THREE = 6391
    REDIS_PASSWORD_THREE = 'IUT807ogjbkaoi'
    
    # sparkSession
    spark = get_spark_session()    
    
    run()
    
    print '计算时间: ' + str(datetime.datetime.today())