
# -*- coding: utf-8 -*-
#!/usr/bin/python
import pysftp
import config_connect
import send_to_carbon
import os
import timeit
try:
    import ConfigParser as cfp
except:
    import configparser as cfp
import io
import time
import logging

# Load configuration file
with open("qos_test.ini") as f:
    ini_config = f.read()
config = cfp.RawConfigParser(allow_no_value=True)
config.readfp(io.BytesIO(ini_config))

# Get setup information from ini file
testFile = config.get('qostest', 'testFile')
prefix = config.get('qostest','prefix')
uuid = config.get('qostest','uuid')
namespace = config.get('qostest','namespace')
sftpPort = config.get('qostest','sftp_port')
sftpPath = config.get('qostest','sftp_path')
locality = config.get('qostest','probe_id')

graphPrefix = prefix+"."+uuid+"."+locality+"."

cnopts = pysftp.CnOpts();
cnopts.hostkeys = None

testfilesize = os.path.getsize(testFile)
print testfilesize

try:
    sftp = pysftp.Connection(host=config_connect.mfhost, username=config_connect.connect_domain+":"+
                                                             config_connect.connect_user,
                        password=config_connect.connect_password,port=int(sftpPort), cnopts=cnopts)
    sftp.chdir(sftpPath)
    print (sftp.getcwd())
    # sftp.put(localpath=testFile)
    # print (sftp.listdir(sftp.getcwd()))
    # fname = testFile.split("/")
    # print fname[-1]
    # print testFile
    # sftp.get(localpath=testFile,remotepath=sftpPath+"/"+fname[-1])

    def puttest():
        sftp.put(localpath=testFile)

    puttime = timeit.timeit(puttest,number=1)
    print puttime

    putrate = (testfilesize/puttime)

    # Build tuple for (sftp put)
    now = int(time.time())
    tuples = ([])
    tuples.append((unicode(graphPrefix + 'sftp.put.size.bytes'), (now, unicode(testfilesize))))
    tuples.append((unicode(graphPrefix + 'sftp.put.time.sec'), (now, unicode(puttime))))
    tuples.append((unicode(graphPrefix + 'sftp.put.speed.bs'), (now, unicode(putrate))))

    print tuples
    # Send to carbon server
    send_to_carbon.sendtocarbon(tuples)



    fname = testFile.split("/")
    def gettest():
        sftp.get(localpath=testFile, remotepath=sftpPath+"/"+fname[-1])


    gettime = timeit.timeit(gettest, number=1)
    print gettime

    getrate = (testfilesize/gettime)

    # Build tuple for (sftp get)
    now = int(time.time())
    tuples = ([])
    tuples.append((unicode(graphPrefix + 'sftp.get.size.bytes'), (now, unicode(testfilesize))))
    tuples.append((unicode(graphPrefix + 'sftp.get.time.sec'), (now, unicode(gettime))))
    tuples.append((unicode(graphPrefix + 'sftp.get.speed.bs'), (now, unicode(getrate))))

    print tuples
    # Send to carbon server
    send_to_carbon.sendtocarbon(tuples)

finally:
    if sftp:
        if sftp.isfile(remotepath=sftpPath+"/"+fname[-1]):
            sftp.remove(remotefile=sftpPath+"/"+fname[-1])
        sftp.close()

