# -*- coding: utf-8 -*-
"""
Created on Tue Aug 01 13:19:02 2017

@author: Administrator
"""
import time
from threading import Thread
import networkx as nx
from neo4j.v1 import GraphDatabase
from operator import methodcaller
import datetime
import numpy as np
from collections import Counter, defaultdict
import sys


class MyThread(Thread):

    def __init__(self, obj, feature_index):
        Thread.__init__(self)
        self.obj = obj
        self.feature_index = feature_index

    def run(self):
        self.result = methodcaller(
            'get_feature_{0}'.format(self.feature_index))(self.obj)

    def get_result(self):
        return self.result


class MyThread2(Thread):

    def __init__(self, driver, name, cypher):
        Thread.__init__(self)
        self.driver = driver
        self.name = name
        self.cypher = cypher

    def run(self):
        self.result = get_graph_data(self.driver, self.name, self.cypher)

    def get_result(self):
        return self.result


class MyTimer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
 
    def __enter__(self):
        self.start = time.time()
        return self
 
    def __exit__(self, *unused):
        self.end = time.time()
        self.secs = self.end-self.start
        self.msecs = self.secs
        if self.verbose:
            print"elapsed time: %f s" % self.msecs


def get_graph_data_2(driver, name):
    with driver.session(max_retry_time=3) as session:
        with session.begin_transaction() as tx:
            nodes = tx.run(
                '''
                match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-
                [:INVEST|SUPERVISOR|DIRECTOR|LEGAL|EXECUTIVE|BRANCH*1..6]-(b) 
                with nodes(p) as np UNWIND np AS x 
                with DISTINCT x
                RETURN x
                ''',
                bbd_qyxx_id=bbd_qyxx_id)
            edges = tx.run(
                '''
                match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-
                [:INVEST|SUPERVISOR|DIRECTOR|LEGAL|EXECUTIVE|BRANCH*1..6]-(b) 
                with relationships(p) as np UNWIND np AS x 
                with DISTINCT x
                RETURN x
                ''',
                bbd_qyxx_id=bbd_qyxx_id)
    return nodes, edges


def get_graph_data(driver, name, cypher):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            result = tx.run(cypher, name=name)
    return result


def format_graph_data(result1, result2):
    """
    构建属性图
    """
    
    def init_graph(edge_list, node_list, is_directed=0):
        # 网络初始化
        if is_directed == 1:
            G = nx.DiGraph()
        elif is_directed == 2:
            G = nx.Graph()

        # 增加带属性的节点
        for node in node_list:
            G.add_node(node[0], **node[1])
        # 增加带属性的边
        G.add_edges_from(edge_list)
        return G
        
    def get_node(row):
        row['x'].properties['labels'] = list(each_row['x'].labels)
        return row['x'].id, row['x'].properties

    def get_edge(row):
        row['x'].properties['type'] = each_row['x'].type
        return row['x'].start, row['x'].end, row['x'].properties

    company_correlative_nodes = [
        get_node(each_row)
        for each_row in result1
    ]
    company_correlative_edges = [
        get_edge(each_row)
        for each_row in result2
    ]

    dig = init_graph(
            company_correlative_edges,
            company_correlative_nodes, is_directed=1)

    g = init_graph(
            company_correlative_edges, 
            company_correlative_nodes, is_directed=2)
    
    return dig, g


class FeatureConstruction(object):
    """
    计算特征的函数集
    """

    def __fault_tolerant(func):
        """
        一个用于容错的装饰器
        """
        @classmethod
        def wappen(cls, *args, **kwargs):
            try:
                return func(cls, *args, **kwargs)
            except Exception, e:
                return (
                    "{func_name} has a errr : {excp}"
                ).format(func_name=func.__name__, excp=e)
        return wappen    

    @classmethod
    def get_feature_1(cls):
        """
        企业背景风险
        """
        one_relation_set = [
            node 
            for node, attr in cls.DIG.nodes(data=True)
            if cls.distance_dict[node] == 1]
        shareholder = [
            src_node for src_node, des_node, edge_attr 
            in cls.DIG.edges(one_relation_set, data=True) 
            if des_node == cls.tarcompany 
            and edge_attr.get('isinvest', '1') == '1']
        os_shareholder = [
            node for node in shareholder 
            if cls.distance_dict[node] == 1
            and cls.DIG.node[node].get('isSOcompany', '') is True 
            and cls.DIG.node[node].get('is_human', '') is False]
        ipo_company = [
            node for ndoe in shareholder
            if cls.DIG.node[node].get('is_ipo', '') != '-']
        out_degree_obj = cls.DIG.out_degree()
        if isinstance(out_degree_obj, dict):
            out_degree_list = out_degree_obj.values()
            out_degree_list.sort()
        else:
            out_degree_list = [0]
        
        is_ipo_tarcompany = 1 
        is_common_interests = 1 
        is_common_address = 1 
        
        x = len(one_relation_set)
        v = len(os_shareholder)
        n = len(ipo_company)
        r_i = is_ipo_tarcompany
        r_1 = is_common_interests
        r_2 = is_common_address
        r_3 = out_degree_list[-1]
        r_4 = sum(out_degree_list[-3:])

        return dict(
            x=x,
            v=v,
            n=n,
            r_1=r_1,
            r_2=r_2,
            r_3=r_3,
            r_4=r_4,
            r_i=r_i
        )

    @classmethod
    def get_feature_2(cls):
        """
        公司运营持续风险
        """
        import math
        
        def get_date(date):
            try:
                return datetime.datetime.strptime(date, '%Y-%M-%d').date()  
            except:
                return datetime.date.today()
        tar_company = get_date(cls.DIG.node[cls.tarcompany].get('esdate', ''))
        
        esdate_relation_set = [
            (datetime.date.today() - get_date(attr.get('esdate', ''))).days 
            for node, attr in cls.DIG.nodes(data=True) 
            if 0 < cls.distance_dict[node] <= 3 
            and attr.get('is_human', '') is False
            and attr.get('esdate', '') != '-']
        t_2 = round(np.average(esdate_relation_set), 2)
        
        if cls.DIG.node.get(cls.tarcompany, 0) and tar_company:
            t_1 = (datetime.date.today() - tar_company).days
        else:
            t_1 = 0
       
        leagal_person_set = [
            (datetime.date.today() - get_date(attr.get('esdate', ''))).days 
            for node, attr in cls.DIG.nodes(data=True) 
            if cls.distance_dict[node] == 1
            and attr.get('is_human', '') is False
            and attr.get('esdate', '') != '-'
        ]
        t_3 = round(np.average(leagal_person_set), 2)
    
        return dict(
            t_2=t_2 if not math.isnan(t_2) else 0,
            t_1=t_1,
            t_3=t_3 if not math.isnan(t_3) else 0,
            y=t_1
        )

    @classmethod
    def get_feature_xgxx(cls):
        """
        【计算基础数据】
        法律诉讼风险：开庭公告、裁判文书、非法集资裁判文书、法院公告、民间借贷
        行政处罚
        被执行风险
        异常经营风险：经营异常、吊销&注销
        银监会行政处罚
        """
        def get_certain_distance_all_info(distance, document_types):
            """
            总条数
            """
            all_array = list()
            # 处理某一个distance不存在节点的情况
            all_array.append([0]*len(document_types))            
            for node, attr in cls.DIG.nodes(data=True):
                if (attr.get('is_human', '') is False
                        and cls.distance_dict[node] == distance):
                    each_array = map(lambda x: x if x else 0, 
                                     [int(attr.get(each_document, '0'))
                                      for each_document in document_types])
                    all_array.append(each_array)
                else:
                    continue
            documents_num = np.sum(all_array, axis=0)
            return documents_num
        
        def get_certain_distance_add_info(distance, document_types):
            """
            出现次数
            """
            all_array = list()
            # 处理某一个distance不存在节点的情况
            all_array.append([0]*len(document_types))            
            for node, attr in cls.DIG.nodes(data=True):
                if (attr.get('is_human', '') is False
                        and cls.distance_dict[node] == distance):
                    each_array = map(lambda x: 1 if x else 0, 
                                     [int(attr.get(each_document, '0'))
                                      for each_document in document_types])
                    all_array.append(each_array)
                else:
                    continue
            documents_num = np.sum(all_array, axis=0)
            return documents_num        
        
        matrx = dict()
        xgxx_type = ['ktgg', 'zgcpwsw', 'rmfygg', 
                     'xzcf', 'zhixing', 
                     'dishonesty', 'jyyc']
        
        xgxx_add_type = ['ktgg_1', 'zgcpwsw_1', 'rmfygg_1', 
                         'xzcf_1', 'zhixing_1', 
                         'dishonesty_1', 'jyyc_1']

        for each_distance in xrange(0, 7):
            xgxx_num_list = get_certain_distance_all_info(each_distance, 
                                                          xgxx_type)
            xgxx_add_num_list = get_certain_distance_add_info(each_distance, 
                                                              xgxx_type)
            matrx[each_distance] = dict(zip(xgxx_type, xgxx_num_list) +
                                        zip(xgxx_add_type, xgxx_add_num_list))
            
        return matrx
    
    @classmethod
    def filter_xgxx_type(cls, tar_xgxx):
        """
        过滤相关信息
        """
        risk = dict()
        for each_distance, xgxx_statistics in cls.xgxx_distribution.iteritems():
            tar_xgxx_statistics = dict([
                    (each_xgxx_type, each_xgxx_num)
                    for each_xgxx_type, each_xgxx_num 
                    in xgxx_statistics.iteritems() 
                    if each_xgxx_type in tar_xgxx])
            risk[each_distance] = tar_xgxx_statistics
        return risk        

    @classmethod
    def get_feature_3(cls):
        """
        1、法律诉讼风险：开庭公告、裁判文书、法院公告、民间借贷
        2、某度关联方中非法集资裁判文书总数
        """
        tar_xgxx = ['ktgg', 'zgcpwsw', 'rmfygg',
                    'ktgg_1', 'zgcpwsw_1', 'rmfygg_1']
        risk = cls.filter_xgxx_type(tar_xgxx)
        
        risk['z'] = round(
            np.dot(
                map(sum, [
                        risk[each_distance].values() 
                        for each_distance in xrange(0, 7)]), 
                [1., 1/2., 1/3., 1/4., 1/5., 1/6., 1/7.]), 2)
        
        return risk

    @classmethod
    def get_feature_4(cls):
        """
        行政处罚风险
        """
        tar_xgxx = ['xzcf', 'xzcf_1']
        risk = cls.filter_xgxx_type(tar_xgxx)
        
        risk['z'] = round(
            np.dot(
                map(sum, [risk[each_distance].values()
                          for each_distance in xrange(0, 7)]),
                [1., 1/2., 1/3., 1/4., 1/5., 1/6., 1/7.]), 2)
        
        return risk
    
    @classmethod
    def get_feature_5(cls):
        """
        被执行风险：被执行、失信被执行
        """
        tar_xgxx = ['zhixing', 'dishonesty', 'zhixing_1', 
                    'dishonesty_1']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['z'] = round(
            np.dot(
                map(
                    lambda x: x['zhixing'] + 2. * x['dishonesty'], 
                    [risk[each_distance] for each_distance in xrange(0, 7)]), 
                [1., 1/2., 1/3., 1/4., 1/5., 1/6., 1/7.]), 2)     
        
        return risk
        
    @classmethod
    def get_feature_6(cls):
        """
        异常经营风险
        """
        tar_xgxx = ['jyyc', 'jyyc_1']
        risk = cls.filter_xgxx_type(tar_xgxx)        
        
        risk['z'] = round(
            np.dot(
                map(
                    lambda x: x['jyyc'], 
                    [risk[each_distance] for each_distance in xrange(0, 7)]), 
                [1., 1/2., 1/3., 1/4., 1/5., 1/6., 1/7.]), 2)
        
        return risk        
        
    @__fault_tolerant
    def get_feature_7(cls):
        """
        实际控制人风险
        """
        def get_degree_distribution(is_human):
            some_person_sets = [
                node
                for node, attr in cls.DIG.nodes(data=True) 
                if attr.get('is_human', '') == is_human
                and cls.distance_dict[node] <= 3]
            person_out_degree = cls.DIG.out_degree(some_person_sets).values()
            if not person_out_degree:
                person_out_degree.append(0)
            return person_out_degree
        
        nature_person_distribution = get_degree_distribution('1')
        legal_person_distribution = get_degree_distribution('0')
        
        nature_max_control = max(nature_person_distribution)
        legal_max_control = max(legal_person_distribution)        
        nature_avg_control = round(np.average(nature_person_distribution), 2)
        legal_avg_control = round(np.average(legal_person_distribution), 2)
        
        total_legal_num = len([
                node 
                for node, attr in cls.DIG.nodes(data=True) 
                if attr.get('is_human', '') is False])
        
        risk = round(((
                    2*(nature_max_control + legal_max_control) + 
                    (nature_avg_control + legal_avg_control)) /
                (2*total_legal_num + 0.001)), 2)
        
        return dict(
            x_1=nature_max_control,
            x_2=legal_max_control,
            y_1=nature_avg_control,
            y_2=legal_avg_control,
            z=total_legal_num,
            r=risk
        )

    @__fault_tolerant
    def get_feature_8(cls):
        """
        公司扩张路径风险
        """
        def get_node_set(is_human):
            return Counter([
                    cls.distance_dict[node]
                    for node, attr in cls.DIG.nodes(data=True)
                    if attr.get('is_human', '') == is_human])
        
        def get_node_set_two(company_type):
            return Counter([
                    cls.distance_dict[node]
                    for node, attr in cls.DIG.nodes(data=True)
                    if attr.get('is_human', '') is False
                    and attr[company_type] != '-'])

        nature_person_distribution = get_node_set('1')
        legal_person_distribution = get_node_set('0')
        so_company_distribution = get_node_set_two('isSOcompany')
        ipo_company_distribution = get_node_set_two('ipo_company')
        
        nature_person_num = [
            nature_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 5)]
        legal_person_num = [
            legal_person_distribution.get(each_distance, 0)
            for each_distance in range(1, 5)]
        so_company_num = [
            so_company_distribution.get(each_distance, 0)
            for each_distance in range(1, 5)]
        ipo_company_num = [
            ipo_company_distribution.get(each_distance, 0)
            for each_distance in range(1, 5)]
        
        risk = round(np.sum(
                np.divide(
                    [
                        np.divide(
                            np.sum(nature_person_num[:each_distance]), 
                            np.sum(legal_person_num[:each_distance]), 
                            dtype=float)
                        for each_distance in range(1, 5)], 
                    np.array([1, 2, 3, 4], dtype=float))), 2)
        
        return dict(
            x_1=legal_person_num[0],
            x_2=legal_person_num[1],
            x_3=legal_person_num[2],
            y_1=nature_person_num[0],
            y_2=nature_person_num[1],
            y_3=nature_person_num[2],
            v_1=so_company_num[0],
            v_2=so_company_num[1],
            v_3=so_company_num[2],
            w_1=ipo_company_num[0],
            w_2=ipo_company_num[1],
            w_3=ipo_company_num[2],
            r=risk if risk < 10000 else 0
        )

    @classmethod
    def get_feature_9(cls):
        """
        关联方中心集聚风险
        """
        one_relation_set = [
            node for node, attr in cls.DIG.nodes(data=True) 
            if cls.distance_dict[node] == 1]
        legal_person_shareholder = len([
                src_node 
                for src_node, des_node, edge_attr 
                in cls.DIG.edges(one_relation_set, data=True) 
                if des_node == cls.tarcompany 
                and edge_attr.get('isinvest', '1') == '1'
                and cls.DIG.node[src_node].get('is_human', '') is False])
        legal_person_subsidiary = len([
                des_node 
                for src_node, des_node, edge_attr 
                in cls.DIG.edges(data=True) 
                if src_node == cls.tarcompany 
                and edge_attr.get('isinvest', '1') == '1'
                and cls.DIG.node[des_node].get('is_human', '') is False])
        
        risk = (
            legal_person_subsidiary -
            legal_person_shareholder)
        
        return dict(
            x=legal_person_subsidiary,
            y=legal_person_shareholder,
            r=risk
        )          

    @classmethod
    def get_feature_10(cls):
        """
        关联方结构稳定风险
        """
        def get_relation_num():
            return Counter([
                    cls.distance_dict[node]
                    for node, attr in cls.DIG.nodes(data=True)])
        
        # 目标企业各个度的节点的数量
        relations_num = get_relation_num()
        relation_three_num = relations_num.get(3, 0)
        relation_two_num = relations_num.get(2, 0)
        relation_one_num = relations_num.get(1, 0)
        relation_zero_num = relations_num.get(0, 1)
        
        x = np.array([
                relation_zero_num, 
                relation_one_num, 
                relation_two_num, 
                relation_three_num]).astype(float)
        
        y_2 = x[2] / (x[1]+x[2])
        y_3 = x[3] / (x[1]+x[2]+x[3])
        risk = y_2/2 + y_3/3
        
        return dict(
            x_1=x[1],
            x_2=x[2],
            x_3=x[3],
            y_2=y_2,
            y_3=y_3,
            z=risk if risk else 0
        )        

    @classmethod
    def get_feature_11(cls):
        pass
# ==============================================================================
#         """
#         关联方地址集中度风险
#         """
#         legal_person_address = [
#             attr['address'] 
#             for node, attr in cls.DIG.nodes(data=True) 
#             if cls.distance_dict[node] <= 3 
#             and attr.get('is_human', '') is False]
#         c = Counter(
#             filter(
#                 lambda x: x is not None and len(x) >= 21,  
#                 legal_person_address))
#         n = c.most_common(1)
#         n= n[0][1] if len(n) > 0 else 1
#         risk = n -1
#                  
#         return dict(
#             n=risk,
#             y=risk
#         )
# ==============================================================================

    @classmethod
    def get_feature_12(cls):
        """
        短期逐利风险
        """
        def get_date(date):
            try:
                return datetime.datetime.strptime(date, '%Y-%M-%d').date()
            except:
                return datetime.date.today()
            
        def get_max_established(distance, timedelta):
            """
            获取在某distance关联方企业中，某企业在任意timedelta内投资成立公司数量的最大值
            """
            # 用于获取最大连续时间数，这里i的取值要仔细琢磨一下，这里输入一个时间差序列与时间差
            def get_date_density(difference_list, timedelta):
                time_density_list = []
                for index, date in enumerate(difference_list):
                    if date < timedelta:
                        s = 0
                        i = 1
                        while(s < timedelta 
                                and i <= len(difference_list)-index):
                            i += 1
                            s = sum(difference_list[index:index+i])
                        time_density_list.append(i)
                    else:
                        continue
                return max(time_density_list) if len(time_density_list) > 0 else 0

            # distance所有节点集合
            relation_set = [
                node 
                for node, attr in cls.DIG.nodes(data=True) 
                if cls.distance_dict[node] == distance]
            investment_dict = defaultdict(lambda: [])
            
            if len(cls.DIG.edges) > 1:
                for src_node, des_node, edge_attr in (
                        cls.DIG.edges(relation_set, data=True)):
                    if (edge_attr.get('is_invest', '0') == '1' 
                            and cls.distance_dict[node] == distance
                            and cls.DIG.node[des_node].get('esdate', '') != '-'
                            and cls.DIG.node[src_node].get('is_human', '') is False):
                        # 将所有节点投资的企业的成立时间加进列表中
                        investment_dict[src_node].append(
                            get_date(cls.DIG.node[des_node].get('esdate', '')))
             
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
        
        x = [get_max_established(each_distance, 180) for each_distance in xrange(0, 7)]
        
        legal_person_num = sum([
            1 
            for node, attr in cls.DIG.nodes(data=True) 
            if attr.get('is_human', '') != '0'])
        
        risk = round(
            np.sum(
                np.divide(x, [1, 2, 3, 4, 5, 6, 7], dtype=float)), 2) / legal_person_num * 15
        
        return dict(
            x_0=x[0],
            x_1=x[1],
            x_2=x[2],
            x_3=x[3],
            w=legal_person_num,
            z=risk
        )

    @classmethod
    def get_some_feature(cls, dig, tar_id, distance_dict, feature_nums):
        # 创建类变量
        cls.tarcompany = tar_id
        cls.DIG = dig
        cls.distance_dict = distance_dict
        cls.xgxx_distribution = cls.get_feature_xgxx()
        
        feature_list = [
            ('feature_{0}'.format(feature_index),
             methodcaller('get_feature_{0}'.format(feature_index))(cls))
            for feature_index in feature_nums]
        feature_list.append(('company_name', cls.tarcompany))
        
        return dict(feature_list)

    @classmethod
    def multi_thread_feature(cls, dig, tar_id, distance_dict, feature_nums):
        # 创建类变量
        cls.tarcompany = tar_id
        cls.DIG = dig
        cls.distance_dict = distance_dict
        cls.xgxx_distribution = cls.get_feature_xgxx()
        
        feature_list = [
            ('feature_{0}'.format(feature_index),
             MyThread(cls, feature_index))
            for feature_index in feature_nums]
        map(lambda (k, t): t.start(), feature_list)
        map(lambda (k, t): t.join(), feature_list)
        result = map(lambda (k, t): (k, t.get_result()), feature_list)
        result.append(('company_name', cls.tarcompany))
        
        return dict(result)

# ==============================================================================
# uri = 'bolt://10.28.102.32:7687'
# my_driver = GraphDatabase.driver(uri, auth=("neo4j", "fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfB"))
# 
# from flask import Flask, request, jsonify
# app = Flask(__name__)
# @app.route('/core_kpi/v1/', methods=['GET'])
# def my_web_app():
#     src_time = time.time()
#     name = request.args.get('company_name')
#     result1, result2 = get_graph_data_2(my_driver, name)
#     DIG, G = format_graph_data(result1, result2)
#     
#     # 获得目标公司分布
#     for node, attr in DIG.nodes(data=True):
#         if attr['name'] == name:
#             tar_id = node
#     distance_dict = nx.shortest_path_length(G, source=tar_id)
# 
#     features = FeatureConstruction.get_some_feature(
#             DIG, tar_id, distance_dict, [_ for _ in range(1, 13)]
#         )
#     
#     des_time = time.time()
#     return jsonify(dict(features, **{'cost_time': round(des_time - src_time, 3)}))
# ==============================================================================


if __name__ == '__main__':
    # 在不考虑网络网络的情况下，创建连接的时间在0.7s左右
    
    uri = 'bolt://10.28.102.32:7687'
    my_driver = GraphDatabase.driver(uri, auth=("neo4j", "fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfB"))
    bbd_qyxx_id = 'aa3c994d90c2453d85748625ff5a9e86'

# ==============================================================================
#     #作为web app                
#     #app.debug = True
#     app.run(host='0.0.0.0', port=9001)
# ==============================================================================
    
    # 作为脚本运行
# ==============================================================================
#     name = sys.argv[1].decode('utf-8')
# ==============================================================================
    
# ==============================================================================
#     with MyTimer(True):
#             
#         t1 = MyThread(my_driver,name,cypher_one)
#         t2 = MyThread(my_driver,name,cypher_two)
# 
#         t1.start()  
#         t2.start()  
#         t1.join()  
#         t2.join() 
#         
#         print len(t1.get_result().data())
#         print len(t2.get_result().data())
#     
# ==============================================================================
    
    with MyTimer(True):
        result1, result2 = get_graph_data_2(my_driver, bbd_qyxx_id)

    with MyTimer(True):
        DIG, G = format_graph_data(result1, result2)
        for node, attr in DIG.nodes(data=True):
            if attr.get('bbd_qyxx_id', '') == bbd_qyxx_id:
                tar_id = node
        distance_dict = nx.shortest_path_length(G, source=tar_id)
        
# ==============================================================================
#     with MyTimer(True):
#         features = FeatureConstruction.multi_thread_feature(
#             DIG, tar_id, distance_dict, [_ for _ in range(1, 13)]
#         )    
#         print features
# ==============================================================================

    with MyTimer(True):
        features = FeatureConstruction.get_some_feature(
            DIG, tar_id, distance_dict, [_ for _ in range(1, 13)]
        )    
        print features

