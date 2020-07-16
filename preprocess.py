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

#生成要load的图，需要的一些预处理的命令
preprocess_commands = 'command/preprocess.txt'

#不断插入新边的命令
insert_commands = 'command/insert.txt'

#前半个图的存放地点
data_to_load = 'base_graph'

#graphflow-shell 路径
shell_path = '~/graphdb/graphflow/build/install/graphflow/bin/graphflow-cli'

#数据文件的分割符
separator = ' '

clause_gen = Clause_generator()

'''
(:organisation)-[:isLocatedIn]->(:place), (:place)-[isPartOf]->(:place);
'''
match_clause = 'Continuously match (m:organisation)-[:isLocatedIn]->(n:place), (n:place)-[:isPartOf]->(r:place) file \"result.txt\";'
save_clause = 'save to dir \"{0}\";'
load_clase = 'load from dir \"{0}\";'

'''
convert all the vertex files to a single cypher commands file
'''
def generate_create_vertex_commands(node_file, save_file):
    count = 0
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline().strip('\n')
        while line:
            id, label = list(map(int, line.split(separator)))
            clause = clause_gen.create_vertex(id, label)
            save_file.write(clause + '\n')
            count += 1
    f.close()
    print('total lines :', count)

def generate_create_edge_commands(edge_file, save_file):
    '''
    for file in edge_files:
        save_file = preprocessing_file
        from_type, edge_type, to_type = filename_to_filetype[file]
        from_offset = vertex_to_idoffset[from_type]
        to_offset = vertex_to_idoffset[to_type]
        total_lines = get_file_lines(dir+file)
        count = 0
        with open(dir+file, 'r', encoding='utf-8') as f:
            line = f.readline()
            line = f.readline().strip('\n')
            while line:
                from_id, to_id = list(map(int, line.split(separator)))
                clause = clause_gen.create_edge(from_id+from_offset, from_type,
                                                edge_type,
                                                to_id+to_offset, to_type)
                save_file.write(clause+'\n')
                line = f.readline().strip('\n')
                count += 1
                if count > 5:
                    save_file = insert_file
        f.close()
    '''

'''
execute the command file generated using " echo command | shell "
'''
echo_statement = 'echo \"{0}\"'
def echo_exec(clauses):
    clause = ' '.join(clauses).replace("\"", "\\\"")
    os.system(echo_statement.format(clause) + '|' + shell_path)

def exec(command_file, batch = 1000):# 1000句一起执行
    with open(command_file, 'r', encoding='utf-8') as file:
        line = file.readline()
        clauses = []
        while line:
            if len(clauses) < batch:
                clauses.append(line)
            else:
                echo_exec(clauses)
                clauses.clear()
            line = file.readline()
        if len(clauses) > 0: echo_exec(clauses)
    file.close()

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

    preprocess_file = open(preprocess_commands, 'w', encoding='utf-8')
    generate_create_vertex_commands(dir+nodes, preprocess_file)
    preprocess_file.close()





