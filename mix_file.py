dir = "command_old/"
file1 = open(dir+"stream_command_bz_10.txt", "r", encoding="utf-8")
file2 = open(dir+"stream_command_bz_100.txt", "r", encoding="utf-8")
file3 = open(dir+"stream_command_bz_1000.txt", "r", encoding="utf-8")

file4 = open(dir+"stream_command_bz_mixed.txt", "w", encoding="utf-8")

num1 = 200
skip2 = 20
num2 = 20
skip3 = 4
num3 = 5

for i in range(num1):
    line = file1.readline()
    file4.write(line)
for i in range(skip2):
    line = file2.readline()
for i in range(num2):
    line = file2.readline()
    file4.write(line)
for i in range(skip3):
    line = file3.readline()
for i in range(num3):
    line = file3.readline()
    file4.readline()

file1.close()
file2.close()
file3.close()
file4.close()
