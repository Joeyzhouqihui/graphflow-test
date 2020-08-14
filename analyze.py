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


'''
num1 = 200
num2 = 20
num3 = 5
sum1 = 0
sum2 = 0
sum3 = 0
with open('mix.txt', 'r', encoding='utf-8') as f:
    for i in range(num1):
        line = f.readline()
        sum1 += float(line.strip())
    for i in range(num2):
        line = f.readline()
        sum2 += float(line.strip())
    for i in range(num3):
        line = f.readline()
        sum3 += float(line.strip())
'''


