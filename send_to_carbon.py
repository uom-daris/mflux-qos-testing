import pickle
import pickletools
import struct
import socket
import logging
import io
try:
    import ConfigParser as cfp
except:
    import configparser as cfp

def sendtocarbon(carb_tuples):
    # Load configuration file
    with open("qos_test.ini") as f:
        ini_config = f.read()
    config = cfp.RawConfigParser(allow_no_value=True)
    config.readfp(io.BytesIO(ini_config))

    # Set Carbon server details
    CARBON_SERVER = config.get('qostest', 'CARBON_SERVER')
    CARBON_PICKLE_PORT = config.get('qostest', 'CARBON_PICKLE_PORT')

    # Connect to Carbon server
    sock = socket.socket()
    try:
        sock.connect((CARBON_SERVER, int(CARBON_PICKLE_PORT)))
    except socket.error:
        logging.critical("Cannot connect to Carbon. Exception thrown: ")
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is carbon-cache.py running?" %
                         {'server': CARBON_SERVER, 'port': int(CARBON_PICKLE_PORT)})
    except Exception as e:
        logging.critical("Cannot connect to Carbon. Unexpected exception thrown: " + str(e))
        raise

    # Create package and send it to Carbon
    package = pickle.dumps(carb_tuples, protocol=2)
    carb_size = struct.pack('!L', len(package))
    sock.sendall(carb_size)
    sock.sendall(package)
    pickletools.dis(package)
    return