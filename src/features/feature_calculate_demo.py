# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase
import networkx as nx
import json


def get_graph_data_2(driver, bbd_qyxx_id):
    with driver.session(max_retry_time=3) as session:
        with session.begin_transaction() as tx:
            nodes = tx.run(
                '''
                match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-[:IS|OF|VIRTUAL|DISHONESTY|XGXX_SHANGBIAO|SHGY_ZHONGBJG|SHGY_ZHAOBJG|QYXX_ZHUANLI|QYXG_QYQS|QYXG_JYYC|KTGG|DCOS|RJZZQ|RMFYGG|RECRUIT|TDDKGS|TDDY|XZCF|ZGCPWSW|ZHIXING|ZPZZQ*1..4]-(b) 
                with nodes(p) as np UNWIND np AS x 
                with DISTINCT x
                RETURN x
                ''',
                bbd_qyxx_id=bbd_qyxx_id)
            edges = tx.run(
                '''
                match p=(a:Company {bbd_qyxx_id: {bbd_qyxx_id}})-[:IS|OF|VIRTUAL|DISHONESTY|XGXX_SHANGBIAO|SHGY_ZHONGBJG|SHGY_ZHAOBJG|QYXX_ZHUANLI|QYXG_QYQS|QYXG_JYYC|KTGG|DCOS|RJZZQ|RMFYGG|RECRUIT|TDDKGS|TDDY|XZCF|ZGCPWSW|ZHIXING|ZPZZQ*1..4]-(b) 
                with relationships(p) as np UNWIND np AS x 
                with DISTINCT x
                RETURN x
                ''',
                bbd_qyxx_id=bbd_qyxx_id)
    return nodes, edges
    
    
def format_graph_data(result1, result2):
    '''构建属性图'''
    
    def init_graph(edge_list, node_list, is_directed=0):
        #网络初始化
        if is_directed == 1:
            G = nx.DiGraph()
        elif is_directed == 2:
            G = nx.Graph()

        #增加带属性的节点
        for node in node_list:
            G.add_node(node[0], **node[1])
        #增加带属性的边
        G.add_edges_from(edge_list)
        return G
        
    def get_node(row):
        row['x'].properties['labels'] = list(each_row['x'].labels)
        return (row['x'].id, row['x'].properties)

    def get_edge(row):
        row['x'].properties['type'] = each_row['x'].type
        return (row['x'].start, row['x'].end, row['x'].properties)

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
            company_correlative_nodes, is_directed = 1)

    g = init_graph(
            company_correlative_edges, 
            company_correlative_nodes, is_directed = 2)
    return dig, g, company_correlative_nodes, company_correlative_edges

def get_graph_structure(node_data, edge_data, out_file):
    result = dict(
        node_data=node_data,
        edge_data=edge_data
    )
    with open(out_file, 'w') as f:
        f.write(json.dumps(result, indent=4))
    

if __name__ == '__main__':
    #在不考虑网络网络的情况下，创建连接的时间在0.7s左右    
    
    uri = 'bolt://10.28.102.32:7687'
    my_driver = GraphDatabase.driver(uri, auth=("neo4j", "fyW1KFSYNfxRtw1ivAJOrnV3AKkaQUfB"))
    bbd_qyxx_id = '2eb36cee3b8c4e02a823e3641203f54e'
    
    # 生成图，同时返回节点与边的列表
    nodes, edges = get_graph_data_2(my_driver, bbd_qyxx_id)
    dig, g, node_data, edge_data = format_graph_data(nodes, edges)


    # 计算距离
    for node, attr in dig.nodes(data=True):
        if attr.get('bbd_qyxx_id', '') == bbd_qyxx_id:
            tar_id = node
    distance_dict = nx.shortest_path_length(g, source=tar_id)
    
    
    # 将图结构保存成一个json
    out_file='graph_structure_20170926.json'
    get_graph_structure(node_data, edge_data, out_file)
    
    