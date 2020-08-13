import os
import re
from clause_generator import *
import heapq
import random

'''
此脚本主要做以下的事情
1. 生成预处理命令并执行，预处理做的工作就是 create 所有的节点，还有一半的边
2. 将预处理命令生成的图 save 进指定目录
3. 生成一个实验命令，这个命令一开始会 load 上面保存的图数据，然后运行一句continuously match语句，然后逐条 create 剩下的一半边
'''

'''
要导入的图数据
node : node id + node label
edge : src node id + edge label + dst node id
'''
dir = '/data/share/users/legend/project/query-generator/WukongSLarge/'
base_edges = 'base_edges.txt'
nodes = 'node.txt'
stream_edges = 'stream_edges.txt'
query = 'query.txt'

#id -> label
pattern = re.compile(r'\d+')
dict = {}

#生成要load的图，需要的一些预处理的命令
base_command_file = 'command/base_command.txt'

#match命令
match_command_file =  'command/match_command_num_{0}.txt'

#不断插入新边的命令
stream_command_file = 'command/stream_command_bz_{0}.txt'

#前半个图的存放地点
data_to_load = 'base_graph'

#graphflow-shell 路径
shell_path = '~/graphdb/graphflow/build/install/graphflow/bin/graphflow-cli'

#数据文件的分割符
separator = ' '

clause_gen = Clause_generator()
var_gen = Variable_generator()

#match 语句所需的点的label和边的label
required_node_labels = set()
required_edge_types = set()

'''
(:organisation)-[:isLocatedIn]->(:place), (:place)-[isPartOf]->(:place);
'''
match_clause = 'Continuously match (m:organisation)-[:isLocatedIn]->(n:place), (n:place)-[:isPartOf]->(r:place) file \"result.txt\";'
save_clause = 'save to dir \"base_graph\";'
load_clase = 'load from dir \"base_graph\";'

def alter_type(type):
    return '_' + type

def choose_edges(base_file, base_num, stream_file, stream_num):
    with open(base_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            dict[from_id] = None
            dict[to_id] = None
            count += 1
            if count >= base_num: break
            line = f.readline()
        f.close()
    with open(stream_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            dict[from_id] = None
            dict[to_id] = None
            count += 1
            if count >= stream_num: break
            line = f.readline()
        f.close()

def choose_edges_v2(base_file, base_num1, base_num2, stream_file, stream_num1, stream_num2):
    with open(base_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count1 = 0
        count2 = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            if edge_type in required_edge_types:
                if count2 < base_num2:
                    count2 += 1
                    dict[from_id] = None
                    dict[to_id] = None
            elif count1 < base_num1:
                count1 += 1
                dict[from_id] = None
                dict[to_id] = None
            if count1 >= base_num1 and count2 >= base_num2: break
            line = f.readline()
        f.close()
    with open(stream_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count1 = 0
        count2 = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            if edge_type in required_edge_types:
                if count2 < stream_num2:
                    count2 += 1
                    dict[from_id] = None
                    dict[to_id] = None
            elif count1 < stream_num1:
                count1 += 1
                dict[from_id] = None
                dict[to_id] = None
            if count1 >= stream_num1 and count2 >= stream_num2: break
            line = f.readline()
        f.close()

def count_nodes(node_file):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id, label = list(pattern.findall(line))
            if id in dict.keys():
                count += 1
            line = f.readline()
    f.close()
    print("node num : ", count)

def generate_create_vertex_commands(node_file, save_file, bz = 100):
    nodes = 0
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id, label = list(pattern.findall(line))
            if id in dict.keys():
                label = alter_type(label)
                clause_gen.add_vertex(id, label)
                count += 1
                nodes += 1
                if count >= bz:
                    clause = clause_gen.create_vertex()
                    save_file.write(clause + '\n')
                    count = 0
                dict[id] = label
            line = f.readline()
        if count > 0:
            clause = clause_gen.create_vertex()
            save_file.write(clause + '\n')
    f.close()
    print("nodes num : ", nodes)

def generate_create_edge_commands(edge_file, save_file, num, bz = 100):
    with open(edge_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        edge_count = 1
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            if edge_count <= num:
                from_type =  dict[from_id]
                to_type = dict[to_id]
                edge_type = alter_type(edge_type)
                clause_gen.add_edge(from_id, from_type, edge_type, to_id, to_type)
                count += 1
                if count >= bz:
                    clause = clause_gen.create_edge()
                    save_file.write(clause + '\n')
                    count = 0
            else: break
            line = f.readline()
            edge_count += 1
        if count > 0:
            clause = clause_gen.create_edge()
            save_file.write(clause + '\n')
    f.close()

def generate_create_edge_commands_v2(edge_file, save_file, num1, num2, bz = 100):
    with open(edge_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count1 = 0
        count2 = 0
        count = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            if edge_type in required_edge_types:
                if count2 < num2:
                    count2 += 1
                    from_type = dict[from_id]
                    to_type = dict[to_id]
                    edge_type = alter_type(edge_type)
                    clause_gen.add_edge(from_id, from_type, edge_type, to_id, to_type)
                    count += 1
            elif count1 < num1:
                count1 += 1
                from_type = dict[from_id]
                to_type = dict[to_id]
                edge_type = alter_type(edge_type)
                clause_gen.add_edge(from_id, from_type, edge_type, to_id, to_type)
                count += 1
            if count >= bz:
                clause = clause_gen.create_edge()
                save_file.write(clause + '\n')
                count = 0
            if count1 >= num1 and count2 >= num2: break
            line = f.readline()
        if count > 0:
            clause = clause_gen.create_edge()
            save_file.write(clause + '\n')
    f.close()

def match_preprocess_command(save_file):
    start_id = 0
    node_labels = list(required_node_labels)
    edge_types = list(required_edge_types)
    size = len(node_labels)
    for label in node_labels:
        clause_gen.add_vertex(start_id, label)
        start_id += 1
        clause = clause_gen.create_vertex()
        save_file.write(clause + '\n')
    for type in edge_types:
        clause_gen.add_edge(start_id, node_labels[start_id%size],
                            type,
                            start_id + 1, node_labels[(start_id+1)%size])
        start_id += 2
        clause = clause_gen.create_edge()
        save_file.write(clause + '\n')


def generate_match_command(query_file, save_file, num = None):
    with open(query_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        if num is None:
            query_num = int(pattern.findall(line)[0])
        else: query_num = num
        for i in range(0, query_num):
            line = f.readline()
            node_num, edge_num = list(map(int, pattern.findall(line)))
            node_types = []
            node_ids = []
            for j in range(0, node_num):
                line = f.readline()
                node_types.append(pattern.findall(line)[0])
                node_ids.append(var_gen.get_variable())
            for j in range(0, edge_num):
                line = f.readline()
                from_id, to_id, edge_type = list(pattern.findall(line))
                from_type = alter_type(node_types[int(from_id)])
                to_type = alter_type(node_types[int(to_id)])
                from_id = node_ids[int(from_id)]
                to_id = node_ids[int(to_id)]
                edge_type = alter_type(edge_type)
                clause_gen.add_match_edge(from_id, from_type, edge_type, to_id, to_type)
            save_file.write(clause_gen.create_continuous_edge("result.txt")+'\n')
    f.close()

def get_required_labels_and_types_for_match(query_file, num = None):
    with open(query_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        if num is None:
            query_num = int(pattern.findall(line)[0])
        else: query_num = num
        for i in range(0, query_num):
            line = f.readline()
            node_num, edge_num = list(map(int, pattern.findall(line)))
            node_types = []
            for j in range(0, node_num):
                line = f.readline()
                node_types.append(pattern.findall(line)[0])
            for j in range(0, edge_num):
                line = f.readline()
                from_id, to_id, edge_type = list(pattern.findall(line))
                from_type = alter_type(node_types[int(from_id)])
                to_type = alter_type(node_types[int(to_id)])
                edge_type = alter_type(edge_type)
                required_node_labels.add(from_type)
                required_node_labels.add(to_type)
                required_edge_types.add(edge_type)
        print("required edges : ", required_edge_types)
        print("required nodes : ", required_node_labels)
    f.close()


if __name__ == '__main__' :
    base_num1 = 5000000
    base_num2 = 5000000
    stream_num1 = 500000
    stream_num2 = 500000

    #base graph
    base_file = open(base_command_file, 'w', encoding='utf-8')
    choose_edges_v2(dir + base_edges, base_num1, base_num2, dir + stream_edges, stream_num1, stream_num2)
    generate_create_vertex_commands(dir + nodes, base_file, bz=100)
    print('finish nodes !')
    generate_create_edge_commands_v2(dir + base_edges, base_file, num1=base_num1, num2=base_num2, bz=100)
    print('finish base edge !')
    get_required_labels_and_types_for_match(dir + query, num=1000)
    match_preprocess_command(base_file)
    print('finish preparing for continuously matching !')
    base_file.write(save_clause + '\n')
    base_file.close()

    #match trigger
    bzs = [10, 100, 1000]
    for bz in bzs:
        match_file = open(match_command_file.format(bz), 'w', encoding='utf-8')
        match_file.write(load_clase + '\n')
        generate_match_command(dir + query, match_file, num=bz)
        match_file.close()
    print('finish match !')

    #stream graph
    for bz in bzs:
        stream_file = open(stream_command_file.format(bz), 'w', encoding='utf-8')
        generate_create_edge_commands_v2(dir + stream_edges, stream_file, num1=stream_num1, num2=stream_num2, bz=bz)
        stream_file.close()
    print('finish stream edge !')


