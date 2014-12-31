import paramiko
import time

try:
    print "begin ssh"
    client = paramiko.SSHClient()
    print "SSHClient"
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey_file = 'id_rsa' 
    key = paramiko.RSAKey.from_private_key_file(pkey_file)
    client.connect("192.168.11.55", username="root",password="redhat",pkey=key)
    print "connected"
    cmd = ["cd /home/hive/HiveBaoshanHospital\n","su hdfs\n"]
    ssh = client.invoke_shell()
    time.sleep(0.1)
    for m in cmd:
                print "excuting:" + m
                ssh.send(m)
                time.sleep(0.5)
                buff = ''
                resp = ssh.recv(1024)
                buff +=resp
                print buff          
    cmdMain = "python hivebaoshanhospital.py --max_pool_size=30 --start_date=2013-08-15 --end_date=2013-08-15\n"
    print "excuting main function:" + cmdMain
    ssh.send(cmdMain) 
    buff = ''
    while 1 == 1:
          try:
              if ssh.recv_ready():
                     resp = ssh.recv(999999)
                     buff +=resp
                     print buff
          except Exception as e:
              print ''
          time.sleep(0.5)
    client.close()
except Exception as e:
    print '\tError\n'