
# -*- coding: utf-8 -*-
#!/usr/bin/python
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
import shutil

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
smbpath = config.get('qostest','smb_mount')

graphPrefix = prefix+"."+uuid+"."

testfilesize = os.path.getsize(testFile)
print testfilesize

try:
    def copyto():
        shutil.copy(src=testFile, dst=smbpath)

    copyttime = timeit.timeit(copyto,number=1)
    copytorate = (testfilesize/copyttime)

    # Build tuple for (copy to smb)
    now = int(time.time())
    tuples = ([])
    tuples.append((unicode(graphPrefix + 'smb.copyto.size.bytes'), (now, unicode(testfilesize))))
    tuples.append((unicode(graphPrefix + 'smb.copyto.time.sec'), (now, unicode(copyttime))))
    tuples.append((unicode(graphPrefix + 'smb.copyto.speed.bs'), (now, unicode(copytorate))))

    print tuples
    # Send to carbon server
    # send_to_carbon.sendtocarbon(tuples)

    fname = testFile.split("/")
    def copyfrom():
        shutil.copy(src=smbpath+"/"+fname[-1], dst=testFile)

    copyftime = timeit.timeit(copyfrom, number=1)
    copyfromrate = (testfilesize/copyftime)
    print copyftime

    # Build tuple for (copy from smb)
    now = int(time.time())
    tuples = ([])
    tuples.append((unicode(graphPrefix + 'smb.copyfrom.size.bytes'), (now, unicode(testfilesize))))
    tuples.append((unicode(graphPrefix + 'smb.copyfrom.time.sec'), (now, unicode(copyftime))))
    tuples.append((unicode(graphPrefix + 'smb.copyfrom.speed.bs'), (now, unicode(copyfromrate))))

    print tuples
    # Send to carbon server
    # send_to_carbon.sendtocarbon(tuples)

finally:
    print "Bye"