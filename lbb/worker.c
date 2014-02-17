#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include "lbb.h"

// Maximum window size
#define MAX_WND 3

int main(void) {
  int wnd;
  zmsg_t *msg;
  zframe_t *frame;
  zctx_t *ctx = zctx_new ();
  zlist_t *clients = zlist_new();
  void *broker = zsocket_new (ctx, ZMQ_DEALER);
  zsocket_connect (broker, "ipc://backend.ipc");
  srand(time(NULL));


  for (wnd = 0; wnd < MAX_WND; wnd++) {
    // Tell broker we’re ready for work
    msg = zmsg_new();
    zmsg_pushstr(msg, WORKER_READY);
    zmsg_pushstr(msg, "");
    // Msg format: [WORKER ID] -> [] -> [READY]
    zmsg_send(&msg, broker);
    printf("Worker: sent READY to broker\n");
  }

  zpoller_t *poller = zpoller_new (broker, NULL);
  assert (poller);

  while (true) {
    // Reply randomly to simulate background work
    if (zlist_size(clients) && rand() % 100000 == 0) {
      msg = zmsg_new();

      // Msg format: [WORKER ID] -> [] -> [CLIENT_ID] -> [] -> [REP DATA]
      zmsg_pushstr(msg, "OK");
      zmsg_wrap(msg, zlist_pop(clients));
      zmsg_pushstr(msg, "");
      zmsg_send (&msg, broker);
      printf("Worker replies\n");
      wnd++;
    }

    // Non-blocking receive
    void *which = zpoller_wait (poller, 0);

    
    if (zpoller_expired (poller) == false && which == broker) {
      // Msg format: [] -> [CLIENT ID] -> [] -> [REQ DATA]
      msg = zmsg_recv (broker);
      wnd--;
      printf("Worker got request\n");

      // Interrupted
      if (!msg)
        break;

      // Discard first empty frame
      frame = zmsg_pop(msg);
      zframe_destroy(&frame);

      // Save client identity
      zlist_append (clients, zmsg_unwrap (msg));
      zmsg_destroy (&msg);

    } else if (zpoller_expired (poller) == false && which != broker) {
      break;
    }

  }
  zctx_destroy (&ctx);
  return 0;
}
