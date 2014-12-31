import pyhs2

class hivehelper:
    """hive data operator helper
       Author:lhan,Created date:2014/12/23
    """

    def __init__(self,database_name):
        conn = pyhs2.connect(host='192.168.11.55',
                   port=10000,
                   authMechanism="PLAIN",
                   user='hdfs',
                   password='',
                   database= database_name)
        self.cur=conn.cursor()

    def sql_excute(self,sql):
        self.cur.execute(sql)
        self.cur.getSchema()
        return self.cur.fetch()

    def create_kpi_value_tmp(self):
        sql="CREATE TABLE IF NOT EXISTS `default`.`kpivalue_tmp`  ( \
	            `hospital_name`	string, \
	            `kpi_code`     	string, \
	            `first_date`   	string, \
	            `granularity`  	int, \
	            `kpi_value`    	decimal)\
                partitioned by (dt string)\
                ROW FORMAT DELIMITED fields terminated by ','\
                STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
                TBLPROPERTIES ('numFiles'='1', 'numPartitions'='0', 'numRows'='0', 'rawDataSize'='0', 'totalSize'='920')"
        self.sql_excute(sql)

    def create_kpi_value_tmp_partition(self,datestr):
        sql="alter table kpivalue_tmp add IF NOT EXISTS partition (dt='"+datestr+"') location '"+datestr+"'"
        self.sql_excute(sql)

