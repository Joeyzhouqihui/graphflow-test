import heapq
import re

pattern = re.compile(r'\d+')

node_file = 'test.txt'

def generate_create_vertex_commands_v2(node_file, rate = 1/10):
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            count += 1
            line = f.readline()
    f.close()
    size = int(rate*count)
    heap = []
    with open(node_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        count = 0
        while line:
            id = pattern.findall(line)[0]
            id = -int(id)
            if count >= size:
                if id > heapq.nsmallest(1, heap):
                    heapq.heapreplace(heap, id)
            else:
                heapq.heappush(heap, id)
            line = f.readline()
    f.close()
    print('size : ', size)
    print(-heapq.nsmallest(1, heap)[0])

if __name__ == "__main__":
    generate_create_vertex_commands_v2(node_file)