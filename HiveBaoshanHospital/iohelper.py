#coding=gbk
import os
import csv

class iohelper:

    @staticmethod
    def get_files(path):
        #ͨ��os.walk����path�µ������ļ��к�Ŀ¼��ÿ�α�������һ����Ԫ��
        #��0��Ϊ��ǰĿ¼����1��Ϊ��ǰĿ¼����Ŀ¼�б�������Ϊ��ǰĿ¼�������ļ����б�
        fileswithfullpath=[]
        for item in os.walk(path):
            #���������ļ�
            for file in item[2]:
                #��ȡ��ǰĿ¼�ľ���·�������ڴ��ļ�
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
             