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

def generate_create_vertex_commands(node_file, save_file, bz = 100):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id, label = list(pattern.findall(line))
            if id in dict.keys():
                label = alter_type(label)
                clause_gen.add_vertex(id, label)
                count += 1
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

myset = set()
def check_vertex_labels(node_file):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        while line:
            id, label = list(pattern.findall(line))
            if id in dict.keys():
                label = alter_type(label)
                myset.add(label)
            line = f.readline()
    f.close()
    print("we have labels : ", list(myset))

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
            for j in range(0, node_num):
                line = f.readline()
                node_types.append(pattern.findall(line)[0])
            for j in range(0, edge_num):
                line = f.readline()
                from_id, to_id, edge_type = list(pattern.findall(line))
                from_type = alter_type(node_types[int(from_id)])
                to_type = alter_type(node_types[int(to_id)])
                from_id = var_gen.get_variable()
                to_id = var_gen.get_variable()
                edge_type = alter_type(edge_type)
                clause_gen.add_match_edge(from_id, from_type, edge_type, to_id, to_type)
            save_file.write(clause_gen.create_continuous_edge("result.txt")+'\n')
    f.close()

if __name__ == '__main__' :
    ''' 
    #base graph
    base_file = open(base_command_file, 'w', encoding='utf-8')
    choose_edges(dir + base_edges, 1000000, dir + stream_edges, 100000)
    generate_create_vertex_commands(dir + nodes, base_file, bz=100)
    print('finish nodes !')
    generate_create_edge_commands(dir + base_edges, base_file, num=1000000, bz=100)
    print('finish base edge !')
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
        generate_create_edge_commands(dir + stream_edges, stream_file, num=100000, bz=bz)
        stream_file.close()
    print('finish stream edge !')
    '''

    choose_edges(dir + base_edges, 1000000, dir + stream_edges, 100000)
    check_vertex_labels(dir + nodes)

