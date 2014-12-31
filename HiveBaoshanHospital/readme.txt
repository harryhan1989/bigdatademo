本程序用于宝山项目大数据抽取，数据抽取语言用的hive
程序执行方式，命令行：
python hivebaoshanhospital.py --max_pool_size=30 --start_date=2013-08-15 --end_date=2013-08-15
描述：max_pool_size设置启动线程数，不指定则用默认值，默认值在config.cfg中配置，start_date设定数据抽取的开始日期，如不指定则用当天作为默认值，
end_date设定数据抽取的结束日期，如不指定则用当天作为默认值
你可以根据需要将此命令调用注册成服务，每天定时执行一次当天的抽取

程序执行需要：python2.6
python执行依赖包：ecdsa,papamiko,pycrypto,pyhs2,python-dateutil,six,threadpool,thrift
以上依赖包可以用pip+easy_install安装得到

程序formulas目录是用来存储数据抽取公式的（公式需要是合规的hive语句），请将不同医院数据库抽取的公式分别存储与独立的文件中，文件名与所对应hive中存放相应数据的数据库名相同（建议都用医院简称，方便后续处理）
程序的所有配置信息存放在config.cfg中，可根据需要修改相应配置信息

程序执行后会将结果数据导入到hive default数据库中的kpivalue_tmp表中，这张表如果事先不存在，程序会自动创建，
程序会自动根据当天日期在kpivalue_tmp表中创建以当天日期命名的patition,例如2014-12-31,并将最终数据存储到当天patition中
数据导出到外部数据库时，导出相应patition数据即可