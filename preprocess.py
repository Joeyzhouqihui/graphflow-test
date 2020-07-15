import os
import re
from clause_generator import *

'''
此脚本主要做以下的事情
1. 生成预处理命令并执行，预处理做的工作就是 create 所有的节点，还有一半的边
2. 将预处理命令生成的图 save 进指定目录
3. 生成一个实验命令，这个命令一开始会 load 上面保存的图数据，然后运行一句continuously match语句，然后逐条 create 剩下的一半边
'''

#要导入的图数据
dir = 'dat/'

#生成要load的图，需要的一些预处理的命令
preprocess_commands = 'command/preprocess.txt'

#不断插入新边的命令
insert_commands = 'command/insert.txt'

#前半个图的存放地点
data_to_load = 'pre_data'

#graphflow-shell 路径
shell_path = '~/graphdb/graphflow/build/install/graphflow/bin/graphflow-cli'

#数据文件的分割符
separator = '|'

#用于做id的重新分配
min_id_permit = 0

#识别文件中的数据是边还是点
filename_to_filetype = {}

#对于每个类型的节点，因为id重分配了，所以要记一下id的起始偏移
vertex_to_idoffset = {}

vertex_files = []
edge_files = []
clause_gen = Clause_generator()

'''
(:organisation)-[:isLocatedIn]->(:place), (:place)-[isPartOf]->(:place);
'''
match_clause = 'Continuously match (m:organisation)-[:isLocatedIn]->(n:place), (n:place)-[:isPartOf]->(r:place) file \"result.txt\";'
save_clause = 'save to dir \"{0}\";'
load_clase = 'load from dir \"{0}\";'

'''
get the vertex and edge types from the filename first
'''
def parse_types():
    for filename in os.listdir(dir):
        tmp = filename.split('_')
        if len(tmp) > 4 :
            edge_files.append(filename)
            filename_to_filetype[filename] = tmp[0:3]
        else :
            vertex_files.append(filename)
            filename_to_filetype[filename] = tmp[0]

'''
convert all the vertex files to a single cypher commands file
'''
def generate_create_vertex_commands(save_file):
    global min_id_permit
    for file in vertex_files:
        vertex_type = filename_to_filetype[file]
        if not vertex_type in vertex_to_idoffset.keys():
            vertex_to_idoffset[vertex_type] = min_id_permit

        max_id_assigned = -1
        with open(dir+file, 'r', encoding='utf-8') as f:
            line = f.readline().strip('\n')
            keys = line.split(separator)
            line = f.readline().strip('\n')
            while line:
                values = line.split(separator)
                id = int(values[0]) + min_id_permit
                props = {}
                for i, str_value in enumerate(values[1:]):
                    if re.search('^[0-9]+$', str_value): value = int(str_value)
                    elif re.search('^[0-9]*\.[0-9]*$', str_value): value = float(str_value)
                    else: value = str_value
                    props[keys[i]] = value
                if len(props) == 0: props = None
                clause = clause_gen.create_vertex(id, vertex_type, props)
                save_file.write(clause+'\n')
                if max_id_assigned < id: max_id_assigned = id
                line = f.readline().strip('\n')

        f.close()
        min_id_permit = max_id_assigned + 1

'''
convert all the edge files to a single cypher commands file
the edge here is simple no props considered
'''
def get_file_lines(filename):
    count = 0
    with open(filename, 'r', encoding='utf-8') as f:
        line = f.readline()
        while line:
            count += 1
            line = f.readline()
    f.close()
    return count

def generate_create_edge_commands(preprocessing_file, insert_file):
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
    parse_types()
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

