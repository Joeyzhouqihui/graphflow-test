from preprocess import *

if __name__ == '__main__' :
    batch = input("enter batch size : ")
    if re.search('^[0-9]+$', batch):
        exec(insert_commands, int(batch))
