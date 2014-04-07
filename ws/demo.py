import simplejson as json
from mongrel2 import handler
from mongrel2.request import Request
import wsutil
import sys
import time
import datetime
import re
import zmq
import threading
import string
import struct
from random import randrange
from random import choice
from ctypes import c_char_p
from ctypes import create_string_buffer
from ctypes import c_uint
from ctypes import c_double
from ctypes import c_longlong


def abortConnection(conn,req,reason='none',code=None):
    #print 'abort',conn,req,reason,code
    if code is not None:
        #print "Closing cleanly\n"
        conn.reply_websocket(req,code+reason,opcode=wsutil.OP_CLOSE)
        closingMessages[req.conn_id]=(time.time(),req.sender)
    else:
        conn.reply(req,'')
    print >>logf,'abort',code,reason

def broker_response_to_key_val(parts):
    if (parts[0] == "INSERT"):
        vals = struct.unpack('=2i8qdi', ''.join(parts[1:]))
        names = ["success", "duplicate", "lat_origin", "long_origin",
                "dep_time", "lat_dest", "long_dest", "arr_time",
                "airport_origin", "airport_dest", "weight", "pid"]
    
    elif (parts[0] == "DELETE"):
        vals = struct.unpack('=i8qi', ''.join(parts[1:]))
        names = ["success", "lat_origin", "long_origin",
                "dep_time", "lat_dest", "long_dest", "arr_time",
                "airport_origin", "airport_dest", "pid"]
    
    elif (parts[0] == "FIND"):
        vals = struct.unpack('=i8qdi', ''.join(parts[1:]))
        names = ["success", "lat_origin", "long_origin",
                "dep_time", "lat_dest", "long_dest", "arr_time",
                "airport_origin", "airport_dest", "weight", "pid"]
    
    else:
        vals = struct.unpack('=16qd2i', ''.join(parts[1:19]+parts[-1:]))
        names = ["lat_origin_1", "long_origin_1", "dep_time_1", "lat_dest_1",
                "long_dest_1", "arr_time_1", "airport_origin_1", "airport_dest_1",
                "lat_origin_2", "long_origin_2", "dep_time_2", "lat_dest_2",
                "long_dest_2", "arr_time_2", "airport_origin_2", "airport_dest_2",
                "sum", "num_entriest", "pid"]
        entry_name = ["lat_origin", "long_origin", "dep_time", "lat_dest",
                "long_dest", "arr_time", "airport_origin", "airport_dest", "weight"]
        entries = []

        for i in range(0, vals[-2]):
            entry_idx = 19 + 9*i
            entry_val = struct.unpack('=8qd', ''.join(parts[entry_idx:entry_idx+9]))
            entries.append({entry_name[j]:entry_val[j] for j in xrange(9)})
        names.append('entries')
        vals = vals + tuple([entries])
    
    key_vals = {names[i]:vals[i]  for i in xrange(len(names))}
    key_vals["op"] = parts[0]
    return key_vals


def worker_routine(sender, conn_id, req_url, rep_url, broker_fe_url, pub_url):
    context = zmq.Context.instance()
    max_live = 3
    ws_req = context.socket(zmq.SUB)
    ws_rep = context.socket(zmq.PUSH)
    broker = context.socket(zmq.DEALER)
    rep_sub = context.socket(zmq.SUB)

    ws_req.connect(req_url)
    ws_rep.connect(rep_url)
    broker.connect(broker_fe_url)
    rep_sub.connect(pub_url)
    
    ws_req.setsockopt(zmq.SUBSCRIBE, conn_id)
    ws_req.setsockopt(zmq.SUBSCRIBE, "die")
    rep_sub.setsockopt(zmq.SUBSCRIBE, '')

    poller = zmq.Poller()
    poller.register(broker, zmq.POLLIN)
    poller.register(ws_req, zmq.POLLIN)
    poller.register(rep_sub, zmq.POLLIN)

    ident = [sender, conn_id]
    liveness = max_live
    last_ping = datetime.datetime.now()

    print "Starting thread for connection %s" %(conn_id)

    while True:
        socks = dict(poller.poll(timeout = 5000))
        if ws_req in socks:
            msg_parts = ws_req.recv_multipart(zmq.NOBLOCK)
            if (msg_parts[0] == "die"):
              break

            cmd = msg_parts[1]

            if (cmd in graph_ops):
              print "ws request:"
              print msg_parts
              try:
                if (cmd == "INSERT"):
                  num_args = map(c_longlong, map(int, msg_parts[2:10]))
                  num_args.append(c_double(float(msg_parts[10])))
                else:
                  num_args = map(c_longlong, map(int, msg_parts[2:]))

                # Msg format: [CLIENT ID] -> [] -> [OP] -> [ARG1] -> [ARG2] ...
                broker.send('', zmq.SNDMORE)
                broker.send(create_string_buffer(cmd), zmq.SNDMORE)
                broker.send_multipart(num_args)

              except Exception as e:
                print e
                print "Bad numeric arguments:" + str(msg_parts)

              
            elif (cmd == "pong"):
              liveness = max_live

            elif (cmd == "close"):
              # Client closed WS, exit thread
              print "Client requested WS close on connection %s\n" %(conn_id)
              break

        # Poll timeout, no input from client
        elif (datetime.datetime.now() >
                    last_ping + datetime.timedelta(seconds=1)):
            liveness = liveness - 1

            # No response to pings, assume client is dead
            if (liveness <= 0):
                print "connection %s had not respond to pings; it is dead" %(conn_id)
                ws_rep.send_multipart(ident + ["close"])
                break

            # Ping WS client
            ws_rep.send_multipart(ident + ["ping"])
            last_ping = datetime.datetime.now()

        if broker in socks:
            try:
              parts = broker.recv_multipart()
              if (parts[1] not in graph_ops):
                print "Invalid rep: " + str(parts)
                continue

              ws_rep.send_multipart(ident + ["rep"] + parts[1:])

            except Exception as e:
              print e

        if rep_sub in socks:
          parts = rep_sub.recv_multipart()
          if (ident != parts[0:2]):
            ws_rep.send_multipart(ident + ["pub"] + parts[3:])

    # Close sockets
    ws_req.close()
    ws_rep.close()
    broker.close()
    rep_sub.close()

# Get broker address from command line arguments
if len(sys.argv) < 4:
  print "Need broker frontend address and name server address"
  sys.exit()
broker_fe_url = "tcp://" + sys.argv[1]
broker_pub_url = "tcp://" + sys.argv[2]
name_url = "tcp://" + sys.argv[3]

sender_id = "82209006-86FF-4982-B5EA-D1E29E55D480"
conn = handler.Connection(sender_id, "tcp://127.0.0.1:9999",
                          "tcp://127.0.0.1:9998")
CONNECTION_TIMEOUT=5
closingMessages={}
badUnicode=re.compile(u'[\ud800-\udfff]')

graph_ops = ["INSERT", "DELETE", "FIND", "RANGE"]
name_ops = ["NAME_INSERT", "NAME_LOOKUP"]
error_msg = "Use one of the following: \"INSERT,i,j,w\", \"DELETE,i,j\", \"FIND,i,j\", \"RANGE,i1,j1,i2,j2\""

logf=open('handler.log','wb')
#logf=open('/dev/null','wb')
#logf=sys.stdout

ws_req_url = "inproc://ws_req"
ws_rep_url = "inproc://ws_rep"
rep_pub_url = "inproc://rep_pub"

context = zmq.Context.instance()

# Publish client subscription to worker threads
ws_req = context.socket(zmq.PUB)
ws_req.bind(ws_req_url)

# Pull backend publishment from worker threads
ws_rep = context.socket(zmq.PULL)
ws_rep.bind(ws_rep_url)

# Pub socket to broadcast some results to all active clients
rep_pub = context.socket(zmq.XPUB)
rep_pub.bind(rep_pub_url)

# Sub socket to listen to results from other brokers
peer_sub = context.socket(zmq.XSUB)
peer_sub.connect(broker_pub_url)

# Connect to name server
name_server = context.socket(zmq.REQ)
name_server.connect(name_url)

poller = zmq.Poller()
poller.register(ws_rep)
poller.register(conn.reqs)
poller.register(peer_sub, zmq.POLLIN)
poller.register(rep_pub, zmq.POLLIN)

print "Starting main loop:"
while True:
    now=time.time()
    logf.flush()
    for k,(t,uuid) in closingMessages.items():
        if now > t+CONNECTION_TIMEOUT:
            conn.send(uuid,k,'')

    try:
        socks = dict(poller.poll())
    except:
        print "FAILED RECV"
        ws_req.send("die")
        sys.exit()

    if peer_sub in socks:
        # Forward message with empty sender_id, conn_id and msg type
        parts = peer_sub.recv_multipart()
        print "peer_sub:"
        print parts
        rep_pub.send_multipart(['', '', ''] + parts)

    if rep_pub in socks:
        # Subscription traveling upstream
        parts = rep_pub.recv_multipart()
        peer_sub.send_multipart(parts)


    # Route worker thread messages back to WS client using conn_id & sender_id
    if ws_rep in socks:
        parts = ws_rep.recv_multipart()
        msg_type = parts[2]

        if (msg_type == "ping"):
            conn.send(parts[0], parts[1],
                  handler.websocket_response("", wsutil.OP_PING))

        elif (msg_type == "rep"):
          try:
            key_vals = broker_response_to_key_val (parts[3:])

            # Publish successful insert and delete
            if ((key_vals["op"] == "INSERT" or key_vals["op"] == "DELETE") and key_vals["success"] == 1):
              rep_pub.send_multipart(parts)

            # Send results as JSON
            json_msg = json.dumps(key_vals)
            conn.send(parts[0], parts[1], handler.websocket_response(json_msg))

          except Exception as e:
            print e
            break;

        elif (msg_type == "pub"):
          try:
            key_vals = broker_response_to_key_val (parts[3:])
            json_msg = json.dumps(key_vals)
            conn.send(parts[0], parts[1], handler.websocket_response(json_msg))

          except Exception as e:
            print e
            break;

        elif (msg_type == "close"):
            conn.send(parts[0], parts[1],
                handler.websocket_response('', wsutil.OP_CLOSE))
            print "closed connection"

    if conn.reqs in socks:
        req = Request.parse(conn.reqs.recv())

        if req.is_disconnect():
            #print "DISCONNECTED", req.conn_id
            continue
    
        if req.headers.get('METHOD') == 'WEBSOCKET_HANDSHAKE':
            #print "HANDSHAKE"
            conn.reply(req,
                    '\r\n'.join([
                        "HTTP/1.1 101 Switching Protocols",
                        "Upgrade: WebSocket",
                        "Connection: Upgrade",
                        "WebSocket-Origin: http://localhost:6767",
                        "WebSocket-Location: ws://localhost:6767/sub",
                        "Sec-WebSocket-Accept: %s\r\n\r\n"]) %(req.body))

            # Spawn a worker thread to handler client subscriptions
            thread = threading.Thread(target=worker_routine,
                args=(req.sender, req.conn_id, ws_req_url, ws_rep_url, broker_fe_url, rep_pub_url))
            thread.start()
            conn.reply_websocket(req, "this is a ping from server", wsutil.OP_PING)
            # conn.reply_websocket(req, "this is a pong from server", wsutil.OP_PONG)
            continue
    
        if req.headers.get('METHOD') != 'WEBSOCKET':
            print 'METHOD is Not WEBSOCKET:',req.headers#,req.body
            conn.reply(req,'')
            continue
    
        try:
            #print 'headers',req.headers
            flags = int(req.headers.get('FLAGS'),16)
            fin = flags&0x80==0x80
            rsvd=flags & 0x70
            opcode=flags & 0xf
            wsdata = req.body
            #print fin,rsvd,opcode,len(wsdata),wsdata
            #logf.write('\n')
        except:
            #print "Unable to decode FLAGS"
            abortConnection(conn,req,'WS decode failed')
            #continue
    
        if rsvd != 0:
            abortConnection(conn,req,'reserved non-zero',
                    wsutil.CLOSE_PROTOCOL_ERROR)
            continue
    
        if opcode == wsutil.OP_CLOSE:
            ws_req.send_multipart([req.conn_id, "close", ''])
            if req.conn_id in closingMessages:
                del closingMessages[req.conn_id]
                conn.reply(req,'')
            else:
                conn.reply_websocket(req,wsdata,opcode)
                conn.reply(req,'')
            continue
        if req.conn_id in closingMessages:
            continue
    
        if opcode not in wsutil.opcodes:
            abortConnection(conn,req,'Unknown opcode',
                    wsutil.CLOSE_PROTOCOL_ERROR)
            continue
            
        if (opcode & 0x8) != 0:
            if opcode == wsutil.OP_PING:
                print "got ping"
                opcode = wsutil.OP_PONG
                conn.reply_websocket(req,wsdata,opcode)
    
            if opcode == wsutil.OP_PONG:
                # Keep worker alive
                ws_req.send_multipart([req.conn_id, "pong", ""])

            continue
    

        if(opcode == wsutil.OP_TEXT):
            try:
                x=wsdata.decode('utf-8')
                #Thank you for not fixing python issue8271 in 2.x :(
                if badUnicode.search(x):
                    raise UnicodeError('Surrogates not allowed')
                #for c in x:
                    #if (0xd800 <= ord(c) <= 0xdfff):
                        #raise UnicodeError('Surrogates not allowed')

                # print "handler received WS data: " + wsdata

                clnt_ws_req = wsdata.split(',')
                if (len(clnt_ws_req) < 2):
                    conn.reply_websocket(req, error_msg, opcode)
                    continue

                cmd = clnt_ws_req[0]
                val = clnt_ws_req[1:]

                # Graph interface operations
                if (cmd in graph_ops):
                    ws_req.send_multipart([req.conn_id, cmd] + val)

                # Name server operations; does sync req/rep for now
                elif (cmd in name_ops):
                    name_server.send_multipart(clnt_ws_req)
                    msg_parts = name_server.recv_multipart()
                    success = struct.unpack('=i', ''.join(msg_parts[1]))
                    msg_parts[1] = success[0]
                    
                    names = ["op", "success", "message", "key", "value"]
                    key_vals = {names[i]:msg_parts[i]  for i in xrange(len(msg_parts))}
                    json_msg = json.dumps(key_vals)
                    conn.reply_websocket(req, json_msg, opcode)

                else:
                    conn.reply_websocket(req, error_msg, opcode)
                continue
            except UnicodeError:
                abortConnection(conn,req,'invalid UTF', wsutil.CLOSE_BAD_DATA)
                continue
        conn.reply_websocket(req,wsdata,opcode)
