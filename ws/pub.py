import zmq
import time
from random import randrange

try:
  CTX = zmq.Context()
  pub = CTX.socket(zmq.PUB)
  pub.bind("tcp://*:9990")
  
  while True:
    zipcode = randrange(10000,100000)
    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)
  
    pub.send_multipart([str(zipcode), str(temperature), str(relhumidity)])
    #time.sleep(1)
except KeyboardInterrupt:
  print "Interrupt received"
