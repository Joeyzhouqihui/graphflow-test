import os
import re
from clause_generator import *

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

'''
convert all the vertex files to a single cypher commands file
'''
def generate_create_vertex_commands(node_file, save_file):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        while line:
            id, label = list(map(int, pattern.findall(line)))
            clause = clause_gen.create_vertex(id, label)
            save_file.write(clause + '\n')
            line = f.readline()
            dict[id] = label
    f.close()

def generate_create_edge_commands(edge_file, save_file):
    with open(edge_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        while line:
            from_id, edge_type, to_id = list(map(int, pattern.findall(line)))
            from_type =  dict[from_id]
            to_type = 0 #dict[to_id]
            clause = clause_gen.create_edge(from_id, from_type,
                                            edge_type,
                                            to_id, to_type)
            save_file.write(clause+'\n')
            line = f.readline()
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
                node_types.append(int(pattern.findall(line)[0]))
            for j in range(0, edge_num):
                line = f.readline()
                from_id, to_id, edge_type = list(map(int, pattern.findall(line)))
                from_type = node_types[from_id]
                to_type = node_types[to_id]
                from_id = var_gen.get_variable()
                to_id = var_gen.get_variable()
                clause_gen.add_match_edge(from_id, from_type, edge_type, to_id, to_type)
            save_file.write(clause_gen.create_continuous_edge("result.txt")+'\n')
    f.close()

if __name__ == '__main__' :
    '''
    preprocess_file = open(preprocess_commands, 'w', encoding='utf-8')
    insert_file = open(insert_commands, 'w', encoding='utf-8')
    #先创建节点
    generate_create_vertex_commands(preprocess_file)
    #在第二个插入脚本前加一句load和match
    insert_file.write(load_clase.format(data_to_load) + '\n')
    insert_file.write(match_clause + '\n')
    #补全边
    generate_create_edge_commands(preprocess_file, insert_file)
    #在预处理最后进行一次save
    preprocess_file.write(save_clause.format(data_to_load) + '\n')
    preprocess_file.close()
    insert_file.close()

    exec(preprocess_commands)
    '''

    #base_file = open(base_command_file, 'w', encoding='utf-8')
    #generate_create_vertex_commands(dir+nodes, base_file)
    print('finish nodes ! \n')
    #generate_create_edge_commands(dir+base_edges, base_file)
    print('finish base edge ! \n')
    #base_file.write(save_clause + '\n')
    #base_file.close()

    stream_file = open(stream_command_file, 'w', encoding='utf-8')
    stream_file.write(load_clase + '\n')
    generate_match_command(dir+query, stream_file)
    print('finish continuously match clauses ! \n')
    generate_create_edge_commands(dir+stream_edges, stream_file)
    print('finish stream edge ! \n')
    stream_file.close()


