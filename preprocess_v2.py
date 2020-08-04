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

#不断插入新边的命令
stream_command_file = 'command/stream_command.txt'

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

'''
convert a part of the vertex files to a single cypher commands file
'''
def generate_create_vertex_commands_v1(node_file, save_file, bz = 100, rate = 1/10000):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id, label = list(pattern.findall(line))
            seed = random.random()
            if seed <= rate:
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

def generate_create_vertex_commands_v2(node_file, save_file, bz = 100, rate = 1/100000):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            count += 1
            line = f.readline()
    f.close()
    size = int(rate * count)
    heap = []
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id = pattern.findall(line)[0]
            id = int(id)
            if count >= size:
                if id > heapq.nsmallest(1, heap)[0]:
                    heapq.heapreplace(heap, id)
            else:
                heapq.heappush(heap, id)
                count += 1
            line = f.readline()
    f.close()
    print("node num : ", size)
    barrier = heapq.nsmallest(1, heap)[0]
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id, label = list(pattern.findall(line))
            if int(id) >= barrier:
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
    print("max id is : ", barrier)
    return barrier

def generate_create_vertex_commands_v3(node_file, save_file, bz = 100, rate = 1/100000):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            count += 1
            line = f.readline()
    f.close()
    size = int(rate * count)
    maxheap = []
    minheap = []
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id = pattern.findall(line)[0]
            id = int(id)
            if count >= size:
                if id > heapq.nsmallest(1, maxheap)[0]:
                    heapq.heapreplace(maxheap, id)
                if -id > -heapq.nsmallest(1, minheap)[0]:
                    heapq.heapreplace(minheap, -id)
            else:
                heapq.heappush(maxheap, id)
                heapq.heappush(minheap, -id)
                count += 1
            line = f.readline()
    f.close()
    print("node num : ", size)
    lower_barrier = heapq.nsmallest(1, maxheap)[0]
    upper_barrier = -heapq.nsmallest(1, minheap)[0]
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id, label = list(pattern.findall(line))
            if int(id) >= lower_barrier or int(id) <= upper_barrier:
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
    return lower_barrier, upper_barrier

def generate_create_edge_commands_v1(edge_file, save_file, bz = 100):
    with open(edge_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        total_edge = 0
        count = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            if from_id in dict.keys() and to_id in dict.keys():
                from_type =  dict[from_id]
                to_type = dict[to_id]
                edge_type = alter_type(edge_type)
                clause_gen.add_edge(from_id, from_type, edge_type, to_id, to_type)
                count += 1
                if count >= bz:
                    clause = clause_gen.create_edge()
                    save_file.write(clause + '\n')
                    count = 0
                total_edge += 1
            line = f.readline()
        if count > 0:
            clause = clause_gen.create_vertex()
            save_file.write(clause + '\n')
        print("total edge is : ", total_edge)
    f.close()

def generate_create_edge_commands_v2(edge_file, save_file, barrier, bz = 100):
    with open(edge_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        total_edge = 0
        count = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            if int(from_id) >= barrier and int(to_id) >= barrier:
                from_type =  dict[from_id]
                to_type = dict[to_id]
                edge_type = alter_type(edge_type)
                clause_gen.add_edge(from_id, from_type, edge_type, to_id, to_type)
                count += 1
                if count >= bz:
                    clause = clause_gen.create_edge()
                    save_file.write(clause + '\n')
                    count = 0
                total_edge += 1
            line = f.readline()
        if count > 0:
            clause = clause_gen.create_vertex()
            save_file.write(clause + '\n')
        print("total edge is : ", total_edge)
    f.close()

def generate_create_edge_commands_v3(edge_file, save_file, lower_barrier, upper_barrier, bz = 100):
    with open(edge_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        total_edge = 0
        count = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            if (int(from_id) <= upper_barrier or int(from_id) >= lower_barrier) and (int(to_id) >= lower_barrier or int(to_id) <= upper_barrier):
                from_type =  dict[from_id]
                to_type = dict[to_id]
                edge_type = alter_type(edge_type)
                clause_gen.add_edge(from_id, from_type, edge_type, to_id, to_type)
                count += 1
                if count >= bz:
                    clause = clause_gen.create_edge()
                    save_file.write(clause + '\n')
                    count = 0
                total_edge += 1
            line = f.readline()
        if count > 0:
            clause = clause_gen.create_vertex()
            save_file.write(clause + '\n')
        print("total edge is : ", total_edge)
    f.close()

def generate_match_command(query_file, save_file):
    with open(query_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        query_num = int(pattern.findall(line)[0])
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
    base_file = open(base_command_file, 'w', encoding='utf-8')
    barrier = generate_create_vertex_commands_v2(dir+nodes, base_file, bz=200)
    print('finish nodes ! \n')
    generate_create_edge_commands_v2(dir+base_edges, base_file, barrier, bz=200)
    print('finish base edge ! \n')
    base_file.write(save_clause + '\n')
    base_file.close()

    stream_file = open(stream_command_file, 'w', encoding='utf-8')
    stream_file.write(load_clase + '\n')
    generate_match_command(dir+query, stream_file)
    print('finish continuously match clauses ! \n')
    generate_create_edge_commands_v2(dir+stream_edges, stream_file, barrier, bz=1)
    print('finish stream edge ! \n')
    stream_file.close()
    '''
    base_file = open(base_command_file, 'w', encoding='utf-8')
    lower_barrier, upper_barrier = generate_create_vertex_commands_v3(dir+nodes, base_file, bz=100)
    print('finish nodes ! \n')
    generate_create_edge_commands_v3(dir + base_edges, base_file, lower_barrier, upper_barrier, bz=100)
    print('finish base edge ! \n')
    base_file.write(save_clause + '\n')
    base_file.close()

    stream_file = open(stream_command_file, 'w', encoding='utf-8')
    stream_file.write(load_clase + '\n')
    generate_match_command(dir + query, stream_file)
    print('finish continuously match clauses ! \n')
    generate_create_edge_commands_v1(dir + stream_edges, stream_file, bz=1)
    print('finish stream edge ! \n')
    stream_file.close()

