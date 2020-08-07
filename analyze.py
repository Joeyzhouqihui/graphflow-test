import numpy as np

triggers = [10, 100, 1000]
bzs = [10, 100, 1000]
dir = "result/"
for trigger in triggers:
    for bz in bzs:
        filename = str(trigger) + "_" + str(bz) + ".txt"
        sum = 0
        count = 0
        with open(dir + filename, 'r', encoding='utf-8') as f:
            line = f.readline()
            while line:
                sum += float(line.strip())
                count += 1
                line = f.readline()
        print(filename, " : ", sum/count)


