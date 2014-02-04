import zmq
import time
import string
from random import randrange
from random import choice

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
        msg_parts = pull.recv_multipart(flags=zmq.NOBLOCK)
        # print msg_parts
        msg = msg_parts[0]
        conn_id = msg_parts[1]
        val = msg_parts[2]
        if (msg == "m_lat_f"):
            pub.send_multipart([msg+conn_id])
        if (msg == "m_down_f"):
            reply = ''.join(choice(string.ascii_uppercase + string.digits)
                for x in range(int(val)))
            pub.send_multipart(["start_f"+conn_id])
            pub.send_multipart(["data_f"+conn_id, reply])
            pub.send_multipart(["stop_f"+conn_id])
            # print "sent " + val + " bytes of data"
    except zmq.ZMQError as e:
        if (e.errno == zmq.EAGAIN):
            pass
        else:
            print e

    # pub.send_multipart([byte(zipcode), byte(temperature), byte(relhumidity)])
    #time.sleep(1)
except KeyboardInterrupt:
  print "Interrupt received"
