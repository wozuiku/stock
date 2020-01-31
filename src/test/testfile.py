import os;

if __name__ == "__main__":

    csv_files = []

    for root, dirs, files in os.walk('data'):
        print(root) #当前目录路径
        print(dirs) #当前路径下所有子目录
        csv_files = files #当前路径下所有非目录子文件


    for csv_file in csv_files:
        print(csv_file)