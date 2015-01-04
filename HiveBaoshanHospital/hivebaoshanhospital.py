from hivehelper import hivehelper
from iohelper import iohelper
from utilities import utilities
import re
from multiprocessing.pool import Pool
from functools import partial
from time import sleep
import threading
from getopt import getopt
import sys
from optparse import OptionParser
from datetime import datetime
import subprocess
import os

#utilities.inital_config_file()
tmp_data=[]
formula_default_start_date=utilities.get_config_value('f_tmp_start_date')
formula_default_end_date=utilities.get_config_value('f_tmp_end_date')
def formula_task(formula,database_name,date_granularity):
    global tmp_data
    kpi_code=formula[0:formula.find(',')]
    first_date=date_granularity.get("first")
    granularity=date_granularity.get("granularity")
    kpi_formula=formula[formula.index(',')+1:len(formula)]
    kpi_value=hivehelper(database_name).sql_excute(kpi_formula)
    value=[]
    value.append(database_name)
    value.append(kpi_code)
    value.append(first_date)
    value.append(granularity)
    value.append(kpi_value[0][0])
    return value
    
def formula_task_wrapper(args):
    return formula_task(*args)

def hand_call_back(value):
    global tmp_data
    tmp_data.extend(value)

class hivebaoshanhospital:
    excute_num=0

    def __init__(self, start_date,end_date,max_pool_size):
        self.formula_folder_path=utilities.get_config_value('formula_path')
        self.start_date=start_date
        self.end_date=end_date
        self.max_pool_size=max_pool_size
        self.pool = Pool(max_pool_size)
    
    def manul_multi_time_retrive_kpis(self):
        global tmp_data  
        print self.max_pool_size
        print self.start_date
        print self.end_date
        formula_files=iohelper.get_files(self.formula_folder_path)
        for formula_file in formula_files:
            formulas=iohelper.read_file_line(formula_file.get("dir"),formula_file.get("file"))
            database_name=formula_file.get("file")
            date_granularitis=utilities.get_all_date_granularity(self.start_date,self.end_date)
            self.process_formula(formulas,database_name,date_granularitis)
        while self.excute_num!=len(tmp_data):
            print len(tmp_data)
            print tmp_data
            sleep(10)
        self.pool.close()
        iohelper.writ_file_to_cvs_two_level(utilities.get_config_value('tmp_data_path'),utilities.get_config_value('tmp_data_name'),tmp_data)
        subprocess.Popen("./tmpdata_load_into_hive.sh",shell=True)
        hivehelper("default").sql_excute("LOAD DATA LOCAL INPATH 'HiveBaoshanHospital/tmpdata.csv' OVERWRITE INTO TABLE kpivalue_tmp")
        self.data_importhive_exportmssql()


    def data_importhive_exportmssql(self):
        now_str=str(utilities.format_date(datetime.now()))
        hivehelper("default").create_kpi_value_tmp()
        hivehelper("default").create_kpi_value_tmp_partition(now_str)
        hivehelper("default").sql_excute("LOAD DATA LOCAL INPATH 'HiveBaoshanHospital/"+utilities.get_config_value('tmp_data_name')+"' \
                        OVERWRITE INTO TABLE kpivalue_tmp PARTITION (dt='"+now_str+"')")
        #todo:connect your sqlserver, and filling the data


    def auto_time_retrive_kpis(self):
        ''''''
        #retrun_data=hivehelper("yanghang_test").sql_excute("select ryrq from ZY_BRSYK where ryrq like '201402%'")
        #for data in retrun_data:
        #    print data     

    def formulas_format(self,formula,date_granularity):
        ''''''
        formula=re.sub(r'like\s\'(('+formula_default_start_date+')|('+formula_default_end_date+'))%\'',"between '"+date_granularity.get('first').replace('-','')+ "' and '"+date_granularity.get('last').replace('-','')+"'",formula)
        formula=re.sub(r''+formula_default_start_date+'',date_granularity.get('first'),formula)
        formula=re.sub(r''+formula_default_end_date+'',date_granularity.get('last'),formula)
        return formula


    def process_formula(self,formulas,database_name,date_granularitis):
        ''''''
        for date_granularity in date_granularitis:
            for formula in formulas:
                formula=self.formulas_format(formula,date_granularity)                
                kpi_value_info=self.pool.map_async(formula_task_wrapper,[(formula,database_name,date_granularity)],chunksize=None,callback=hand_call_back)
                self.excute_num=self.excute_num+1

def main():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("--start_date", dest="start_date",type="string",default=str(utilities.format_date(datetime.now())),
                      help="set job start date,input format yyyy-MM-dd ")
    parser.add_option("--end_date", dest="end_date",type="string",default=str(utilities.format_date(datetime.now())),
                    help="set job end date, input format yyy-MM-dd")
    parser.add_option("--max_pool_size", dest="max_pool_size",type="int",default=int(utilities.get_config_value('max_pool_size')),
                    help="set max multi-thread pool size, input(int)")

    (opts, args) = parser.parse_args()

    hivebaoshanhospital(  start_date=opts.start_date,
                          end_date=opts.end_date,
                          max_pool_size=opts.max_pool_size).manul_multi_time_retrive_kpis()

if __name__ == '__main__': 
    main()