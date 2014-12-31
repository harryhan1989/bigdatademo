#coding=gbk
import os
import csv

class iohelper:

    @staticmethod
    def get_files(path):
        #通过os.walk遍历path下的所有文件夹和目录，每次遍历产生一个三元组
        #第0个为当前目录，第1个为当前目录的子目录列表，第三个为当前目录下所有文件的列表
        fileswithfullpath=[]
        for item in os.walk(path):
            #对所有子文件
            for file in item[2]:
                #获取当前目录的绝对路径，用于打开文件
                dir=os.path.abspath(item[0])
                fileswithfullpath.append({"dir":dir,"file":file})
        return fileswithfullpath

    @staticmethod
    def read_file_line(dir,file):
        with open(os.path.join(dir,file),'r') as fi:
            return fi.readlines()

    @staticmethod
    def write_file_line(dir,file,line_data):
        print line_data
        with open(os.path.join(dir,file),'a') as fi:
             fi.writelines(line_data)

    @staticmethod
    def writ_file_to_cvs_base(dir,file,basic_array):
        with open(os.path.join(dir,file), 'wb') as csvfile:
             spamwriter = csv.writer(csvfile, delimiter=',',
                    quotechar=',', quoting=csv.QUOTE_MINIMAL)
             spamwriter.writerow(basic_array)

    @staticmethod
    def writ_file_to_cvs_two_level(dir,file,two_level_array):
         with open(os.path.join(dir,file), 'wb') as csvfile:
             spamwriter = csv.writer(csvfile, delimiter=',',
                    quotechar=',', quoting=csv.QUOTE_MINIMAL)
             for basic_array in two_level_array:
                 spamwriter.writerow(basic_array)
             