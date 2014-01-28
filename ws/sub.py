import simplejson as json
from mongrel2 import handler
from mongrel2.request import Request
import wsutil
import sys
import time
import re
import zmq
import threading
import signal

sender_id = "82209006-86FF-4982-B5EA-D1E29E55D480"

conn = handler.Connection(sender_id, "tcp://127.0.0.1:9999",
                          "tcp://127.0.0.1:9998")

CONNECTION_TIMEOUT=5

closingMessages={}

badUnicode=re.compile(u'[\ud800-\udfff]')

logf=open('echo.log','wb')
#logf=open('/dev/null','wb')
#logf=sys.stdout

def abortConnection(conn,req,reason='none',code=None):
    #print 'abort',conn,req,reason,code
    if code is not None:
        #print "Closing cleanly\n"
        conn.reply_websocket(req,code+reason,opcode=wsutil.OP_CLOSE)
        closingMessages[req.conn_id]=(time.time(),req.sender)
    else:
        conn.reply(req,'')
    print >>logf,'abort',code,reason

def worker_routine(sender, conn_id, cmds_url, relay_url, pub_url):
    context = zmq.Context.instance()
    cmds = context.socket(zmq.SUB)
    be = context.socket(zmq.SUB)
    relay = context.socket(zmq.PUSH)

    cmds.connect(cmds_url)
    relay.connect(relay_url)
    be.connect(pub_url)
    
    cmds.setsockopt(zmq.SUBSCRIBE, conn_id)
    poller = zmq.Poller()
    poller.register(cmds)
    poller.register(be)

    while True:
        socks = dict(poller.poll())
        if cmds in socks:
          msg_parts = cmds.recv_multipart()
          cmd = msg_parts[1]
          if (cmd == "sub"):
            be.setsockopt_string(zmq.SUBSCRIBE, msg_parts[2].decode("ascii"))
            print "connection %s subscribed to '%s'" %(conn_id, msg_parts[2])
          elif (cmd == "unsub"):
            be.setsockopt_string(zmq.UNSUBSCRIBE, msg_parts[2].decode("ascii"))
            print "connection %s unsubscribed '%s'" %(conn_id, msg_parts[2])

        if be in socks:
          msg_parts = be.recv_multipart()
          #print msg_parts
          relay.send_multipart([sender, conn_id] + msg_parts)
    

cmds_url = "inproc://commands"
relay_url = "inproc://relay"
be_url = "tcp://127.0.0.1:9990"

context = zmq.Context.instance()
sub = context.socket(zmq.SUB)
sub.connect(be_url);
sub.setsockopt_string(zmq.SUBSCRIBE, "100:".decode('ascii'))

# Publish client subscription to worker threads
cmds = context.socket(zmq.PUB);
cmds.bind(cmds_url)

# Pull backend publishment from worker threads
relay = context.socket(zmq.PULL);
relay.bind(relay_url)

poller = zmq.Poller();
poller.register(relay);
poller.register(conn.reqs);

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
        sys.exit()

    if relay in socks:
        msg_parts = relay.recv_multipart()
        # print msg_parts
        conn.send(msg_parts[0], msg_parts[1],
              handler.websocket_response(json.dumps(msg_parts[2:])))

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
                args=(req.sender, req.conn_id, cmds_url, relay_url, be_url))
            thread.start()
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
            if opcode ==wsutil.OP_PING:
                opcode = wsutil.OP_PONG
                conn.reply_websocket(req,wsdata,opcode)
    
            continue
    
        if opcode == wsutil.OP_PONG:
            continue # We don't send pings, so ignore pongs

        if(opcode == wsutil.OP_TEXT):
            try:
                x=wsdata.decode('utf-8')
                #Thank you for not fixing python issue8271 in 2.x :(
                if badUnicode.search(x):
                    raise UnicodeError('Surrogates not allowed')
                #for c in x:
                    #if (0xd800 <= ord(c) <= 0xdfff):
                        #raise UnicodeError('Surrogates not allowed')
                clnt_cmds = wsdata.split(':')
                cmd = clnt_cmds[0]
                val = clnt_cmds[1]
                if (cmd == "sub"):
                    cmds.send_multipart([req.conn_id, cmd, val])
                    conn.reply_websocket(req, "Subscribed to '%s'" %(val), opcode)

                elif (cmd == "unsub"):
                    cmds.send_multipart([req.conn_id, cmd, val])
                    conn.reply_websocket(req, "Unsubscribed '%s'" %(val), opcode)

                else:
                    print "be wary"
                    conn.reply_websocket(req, "Usage: '[sub|unsub]:TOPIC'", opcode);
                continue
            except UnicodeError:
                abortConnection(conn,req,'invalid UTF', wsutil.CLOSE_BAD_DATA)
                continue
        conn.reply_websocket(req,wsdata,opcode)
