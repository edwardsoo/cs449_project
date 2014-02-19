#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include "lbb.h"

int main (void)
{
  zctx_t *ctx = zctx_new ();
  zframe_t *identity;
  void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
  void *backend = zsocket_new (ctx, ZMQ_ROUTER);
  zsocket_bind (frontend, "tcp://127.0.0.1:9990");
  zsocket_bind (backend, "ipc://backend.ipc");

  // Queue of available workers
  zlist_t *workers = zlist_new ();
  
  zmq_pollitem_t items [] = {
    { backend, 0, ZMQ_POLLIN, 0 },
    { frontend, 0, ZMQ_POLLIN, 0 }
  };

  while(1) {
    // Poll frontend only if we have available workers
    int rc = zmq_poll (items, zlist_size (workers)? 2: 1, -1);

    // Interrupted
    if (rc == -1)
      break;

    // Handle worker activity on backend
    if (items [0].revents & ZMQ_POLLIN) {

      // Use worker identity for load-balancing
      // Msg format: [CLIENT ID] -> [] -> [...]
      zmsg_t *msg = zmsg_recv (backend);
      printf("Broker: got worker ready for work\n");

      // Interrupted
      if (!msg)
        break;

      // Save worker ID on queue
      identity = zmsg_unwrap(msg);
      zlist_append (workers, identity);

      // Forward message to client if it’s not a READY
      zframe_t *frame = zmsg_first (msg);
      if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0) {
        // Msg format: [READY]
        zmsg_destroy (&msg);
      } else {
        // Msg format: [CLIENT ID] -> [] -> [REP]
        zmsg_dump(msg);
        zmsg_send (&msg, frontend);
      }
    }
    if (items [1].revents & ZMQ_POLLIN) {
      // Got client request
      // Msg format: [CLIENT ID] -> [] -> [REQ]
      zmsg_t *msg = zmsg_recv (frontend);

      if (msg) {
        // Route to first available worker
        identity = (zframe_t *) zlist_pop(workers);
        zmsg_wrap (msg, identity);

        // Msg format: [WORKER ID] -> [] -> [CLIENT ID] -> [] -> [REQ DATA]
        zmsg_send (&msg, backend);
        printf("Broker: route req to a worker\n");
      }
    }
  }

  // When we’re done, clean up properly
  while (zlist_size (workers)) {
    zframe_t *frame = (zframe_t *) zlist_pop (workers);
    zframe_destroy (&frame);
  }
  zlist_destroy (&workers);
  zctx_destroy (&ctx);
  return 0;
}
