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

def choose_edges(edge_file):
    with open(edge_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            from_id, edge_type, to_id = list(pattern.findall(line))
            line = f.readline()
    f.close()

if __name__ == "__main__":
    dict["123"]
    print("123" in dict.keys())
