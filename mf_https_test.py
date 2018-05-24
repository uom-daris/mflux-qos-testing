#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mfclient
import mf_connect
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
namespace = config.get('qostest','namespace')
locality = config.get('qostest','probe_id')


# Create mediaflux connection
try:
    cxn = mf_connect.connect()
except Exception as e:
    logging.critical("Cannot connect to Mediaflux. Exception thrown: " + str(e))
    raise

# Run metric tests, will do a server.ping, asset.create and a asset.get
try:
    # Get the server UUID to identify the MF server
    serverUUID = cxn.execute("server.uuid")
    suuid = serverUUID.value("uuid")
    # print serverUUID
    # print suuid
    # Set the graphite prefix namespace
    graphPrefix = prefix+str(suuid)+".qos"+"."+locality+".https.1."
    # print graphPrefix

    ###
    # Server.ping
    ###
    # Execute server ping test
    pingResults = cxn.execute("server.ping", inputs=[mfclient.MFInput(testFile)])

    # Parse XML test results
    size = pingResults.element("size")
    rate = pingResults.elements("rate")
    rateAttribs = rate[0].attributes()
    rateUnits = rateAttribs["units"]

    sizeAttribs = size.attributes()
    sizeBytes = sizeAttribs["bytes"]

    readtime = pingResults.value("read-time")
    readunits = pingResults.value("read-time/@units")

    # Build tuple for (server.ping)
    now = int(time.time())
    tuples = ([])
    tuples.append((unicode(graphPrefix + 'ping.size.bytes'), (now, unicode(sizeBytes))))
    tuples.append((unicode(graphPrefix + 'ping.speed.bs'), (now, unicode(rate[0].value()))))
    tuples.append((unicode(graphPrefix + 'ping.read.'+readunits), (now, unicode(readtime))))

    # print tuples
    # Send to carbon server
    send_to_carbon.sendtocarbon(tuples)

    ###
    # asset.create
    ###
    # Create arguments for create asset test
    cAsset = mfclient.XmlStringWriter('args')
    cAsset.push("service", attributes={"name":"asset.create"})
    cAsset.add("namespace",namespace)
    cAsset.add("action","get-meta")
    cAsset.pop()
    cAsset.add("time", True)
    create = pingResults

    def createtest():
        global create
        create = cxn.execute("service.execute", cAsset.doc_text(), inputs=[mfclient.MFInput(testFile)])

    pythoncreatetime = timeit.timeit(createtest,number=1)

    # Parse XML test results
    asset = create.element("reply/response/asset")
    assetID = create.value("reply/response/asset/@id")
    content = create.element("reply/response/asset/content")
    size = content.value("size")
    copytime = content.value("copy-ctime")
    timetocopy = create.value("time")
    timetocopyunits = create.value("time/@units")
    storeName = content.value("store")
    # Calculate upload rate in bytes/S
    uploadRate = float(size)/float(pythoncreatetime)

    # Build tuple for (asset.create)
    now = int(time.time())
    tuples = ([])
    tuples.append((unicode(graphPrefix + 'create.size.bytes'), (now, unicode(size))))
    tuples.append((unicode(graphPrefix + 'create.speed.bs'), (now, unicode(uploadRate))))
    tuples.append((unicode(graphPrefix + 'create.timeto.'+timetocopyunits), (now, unicode(timetocopy))))
    tuples.append((unicode(graphPrefix + 'create.pythontime.sec'), (now, unicode(pythoncreatetime))))
    tuples.append((unicode(graphPrefix + 'create.store.name'), (now, unicode(storeName))))

    # print tuples
    # Send to carbon server
    send_to_carbon.sendtocarbon(tuples)


    ###
    # Download asset to a null file
    ###
    dlAsset = mfclient.XmlStringWriter('args')
    dlAsset.push("service", attributes={"name":"asset.get","outputs":"1"})
    dlAsset.add("id", assetID)
    dlAsset.pop()
    dlAsset.add("time",True)
    dlResults = create

    def dltest():
        global dlResults
        dlResults = cxn.execute("service.execute", dlAsset.doc_text(),
                                outputs=[mfclient.MFOutput(file_obj=open(os.devnull, "wb"))])
    # Parse XML test results
    pythondltime = timeit.timeit(dltest,number=1)
    dlAsset = dlResults.element("reply/response/asset")
    dlContent = dlResults.element("reply/response/asset/content")
    dlSize = dlContent.value("size")
    dlCopytime = dlContent.value("copy-ctime")
    timetodownload = dlResults.value("time")
    timetodownloadunits = dlResults.value("time/@units")
    dlStoreName = dlContent.value("store")

    # Calculate download rate
    dlRate = float(dlSize)/float(pythondltime)

    # Build tuple for (asset.get)
    now = int(time.time())
    tuples = ([])
    tuples.append((unicode(graphPrefix + 'get.size.bytes'), (now, unicode(dlSize))))
    tuples.append((unicode(graphPrefix + 'get.speed.bs'), (now, unicode(dlRate))))
    tuples.append((unicode(graphPrefix + 'get.timeto.'+timetodownloadunits), (now, unicode(timetodownload))))
    tuples.append((unicode(graphPrefix + 'get.pythontime.sec'), (now, unicode(pythondltime))))
    tuples.append((unicode(graphPrefix + 'get.store.name'), (now, unicode(dlStoreName))))

    # print tuples
    # Send to carbon server
    send_to_carbon.sendtocarbon(tuples)


    # Remove created asset
    rmAsset = mfclient.XmlStringWriter('args')
    rmAsset.add("id",assetID)
    cxn.execute("asset.destroy",rmAsset.doc_text())

except Exception as e:
    logging.warning("Failed to run metric tests. Exception thrown: " + str(e))
    raise
finally:
    cxn.close()