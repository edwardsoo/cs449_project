#include <zmq.h>
#include <czmq.h>
#include <stdio.h>

int main(void) {
  int req, rep;
  zmsg_t *msg;
  zctx_t *ctx = zctx_new ();
  void *broker = zsocket_new (ctx, ZMQ_DEALER);
  zsocket_connect (broker, "ipc://frontend.ipc");

  req = rep = 0;
  zpoller_t *poller = zpoller_new (broker, NULL);
  while (rep < 10) {
    // Send request, get reply
    if (req < 10) {
      msg = zmsg_new();
      zmsg_pushstr(msg, "HELLO");
      zmsg_pushstr(msg, "");
      zmsg_send(&msg, broker); 
      zstr_send (broker, "HELLO");
      printf("Client: sent req\n");
      req++;
    }

    void *which = zpoller_wait (poller, 0);
    if (zpoller_expired (poller) == false && which == broker) {
      msg = zmsg_recv (broker);
      rep++;

      if (!msg)
        break;

      zframe_t *frame = zmsg_pop(msg);
      zframe_destroy(&frame);
      frame = zmsg_pop(msg);
      zframe_fprint (frame, "Client got req: ", stdout);

      zmsg_destroy(&msg); 

    } else if (zpoller_expired (poller) == false && which != broker) {
      break;
    }
  }
  zctx_destroy (&ctx);
  return 0;
}
