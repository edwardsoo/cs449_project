import zmq
import time
from random import randrange

try:
  context= zmq.Context()
  pub = context.socket(zmq.PUB)
  pull = context.socket(zmq.PULL)
  pub.bind("tcp://*:9990")
  pull.bind("tcp://*:9991")
  
  while True:

    zipcode = randrange(10000,100000)
    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)
  
    pub.send_multipart([str(zipcode), str(temperature), str(relhumidity)])

    try:
        msg = pull.recv(flags=zmq.NOBLOCK)
        if (msg == "m_lat"):
            pub.send_multipart([msg])
    except zmq.ZMQError as e:
        if (e.errno == zmq.EAGAIN):
            pass
        else:
            print e

    # pub.send_multipart([byte(zipcode), byte(temperature), byte(relhumidity)])
    #time.sleep(1)
except KeyboardInterrupt:
  print "Interrupt received"
