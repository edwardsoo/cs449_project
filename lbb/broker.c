#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include "lbb.h"
#define BIND_ADDR "tcp://*"

int main (int argc, char* argv[])
{
  int max_fe, max_be;

  if (argc != 6) {
    printf("Usage: %s TCP_ADDR FRONTEND_PORT BACKEND_PORT MAX_FE_CONN MAX_BE_CONN\n", argv[0]);
    printf("Example: %s 12.34.56.78 9998 9999 1 4\n", argv[0]);
    exit(1);
  }

  max_fe = atoi(argv[4]);
  max_be = atoi(argv[5]);
  if (!max_fe || !max_be) {
    printf("Number of connection must be greater than zero\n");
    exit(1);
  }

  zctx_t *ctx = zctx_new ();
  zframe_t *identity;
  char str[0x100];
  void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
  void *backend = zsocket_new (ctx, ZMQ_ROUTER);

  // zsocket_bind (frontend, "tcp://127.0.0.1:9990");
  // zsocket_bind (backend, "tcp://127.0.0.1:5555");

  strcpy(str, BIND_ADDR ":");
  strcat(str, argv[2]);
  printf("Broker: binding frontend to %s\n", str);
  zsocket_bind (frontend, str);

  strcpy(str, BIND_ADDR ":");
  strcat(str, argv[3]);
  printf("Broker: binding backend to %s\n", str);
  zsocket_bind (backend, str);

  // Notify broker discovery service of this broker instance
  void *push = zsocket_new (ctx, ZMQ_PUSH);
  zsocket_connect (push, "tcp://127.0.0.1:11110");
  strcpy(str, "tcp://");
  strcat(str, argv[1]);
  strcat(str, ":");
  strcat(str, argv[2]);
  printf("Broker: advertising frontend at %s\n", str);
  zmq_send(push, str, strlen(str), ZMQ_SNDMORE);
  zmq_send(push, &max_fe, sizeof(int), ZMQ_SNDMORE);
  strcpy(str, "tcp://");
  strcat(str, argv[1]);
  strcat(str, ":");
  strcat(str, argv[3]);
  printf("Broker: advertising backend at %s\n", str);
  zmq_send(push, str, strlen(str), ZMQ_SNDMORE);
  zmq_send(push, &max_be, sizeof(int), 0);
  

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

      // Interrupted
      if (!msg)
        break;

      // Save worker ID on queue
      identity = zmsg_unwrap(msg);
      zlist_append (workers, identity);
      zframe_print(identity, "Broker: received from worker ");

      // Forward message to client if it’s not a READY
      zframe_t *frame = zmsg_first (msg);
      if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0) {
        // Msg format: [READY]
        zmsg_destroy (&msg);
      } else {
        // Msg format: [CLIENT ID] -> [] -> [REP]
        printf("Broker: route rep to client\n");
        zmsg_dump(msg);
        zmsg_send (&msg, frontend);
      }
    }
    if (items [1].revents & ZMQ_POLLIN) {
      // Got client request
      // Msg format: [CLIENT ID] -> [] -> [REQ]
      zmsg_t *msg = zmsg_recv (frontend);

      if (msg) {
        printf("Broker: received req from a client\n");

        // Route to first available worker
        identity = (zframe_t *) zlist_pop(workers);
        zmsg_wrap (msg, identity);
        zframe_print(identity, "Broker: route req to worker ");
        zmsg_dump(msg);

        // Msg format: [WORKER ID] -> [] -> [CLIENT ID] -> [] -> [REQ DATA]
        zmsg_send (&msg, backend);
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
