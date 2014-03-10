#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <signal.h>
#include "broker.h"
#define TCP_URL "tcp://"

int main (int argc, char* argv[])
{
  zctx_t *ctx;
  zframe_t *identity, *frame;
  zmsg_t* msg;
  char str[0x100];

  if (argc != 5) {
    printf ("Usage: %s HOST_ADDR FRONTEND_PORT BACKEND_PORT DISCOVERY_ADDR_AND_PORT\n",
        argv[0]);
    printf ("Example: %s 1.2.3.4 9998 9999 5.6.4.7:8899\n", argv[0]);
    exit (1);
  }

  ctx = zctx_new ();
  void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
  void *backend = zsocket_new (ctx, ZMQ_ROUTER);

  strcpy (str, TCP_URL "*:");
  strcat (str, argv[2]);
  printf ("Broker: binds frontend to %s\n", str);
  zsocket_bind (frontend, str);

  strcpy (str, TCP_URL "*:");
  strcat (str, argv[3]);
  printf ("Broker: binds backend to %s\n", str);
  zsocket_bind (backend, str);

  // Notify broker discovery service of this broker instance
  void *disc = zsocket_new (ctx, ZMQ_DEALER);
  int linger = -1;
  zmq_setsockopt (disc, ZMQ_LINGER, &linger, sizeof (linger));
  strcpy (str, TCP_URL);
  strcat (str, argv[4]);
  zsocket_connect (disc, str);

  // Ignore interrupts
  // signal(SIGINT, SIG_IGN);

  printf ("Broker: advertises self at %s\n", str);
  msg = zmsg_new ();
  frame = zframe_new (DISC_ADD, strlen (DISC_ADD));
  zmsg_append (msg, &frame);
  strcpy (str, TCP_URL);
  strcat (str, argv[1]);
  strcat (str, ":");
  strcat (str, argv[3]);
  frame = zframe_new (str, strlen (str));
  zmsg_append (msg, &frame);
  zmsg_pushmem (msg, NULL, 0);
  zmsg_send (&msg, disc);

  // Queue of available workers
  zlist_t *workers = zlist_new ();
  
  zmq_pollitem_t items [] = {
    { backend, 0, ZMQ_POLLIN, 0 },
    { frontend, 0, ZMQ_POLLIN, 0 }
  };


  printf ("Broker: enters main loop\n");
  while (1) {
    // Poll frontend only if we have available workers
    int rc = zmq_poll (items, zlist_size (workers)? 2: 1, -1);

    // Interrupted
    if (rc == -1) {
      printf("Broker: poller interrupted\n");
      break;
    }

    // Handle worker activity on backend
    if (items [0].revents & ZMQ_POLLIN) {

      // Use worker identity for load-balancing
      // Msg format: [CLIENT ID] -> [] -> [...]
      msg = zmsg_recv (backend);

      // Interrupted
      if (!msg)
        break;

      // Save worker ID on queue
      identity = zmsg_unwrap (msg);
      zlist_append (workers, identity);
      zframe_print (identity, "Broker: received from worker ");

      // Forward message to client if it’s not a READY
      frame = zmsg_first (msg);
      if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0) {
        // Msg format: [READY]
        zmsg_destroy (&msg);
      } else {
        // Msg format: [CLIENT ID] -> [] -> [REP]
        printf ("Broker: routes rep to client\n");
        zmsg_dump (msg);
        zmsg_send (&msg, frontend);
      }
    }
    if (items [1].revents & ZMQ_POLLIN) {
      // Got client request
      // Msg format: [CLIENT ID] -> [] -> [REQ]
      zmsg_t *msg = zmsg_recv (frontend);

      if (msg) {
        printf ("Broker: received req from a client\n");

        // Route to first available worker
        identity = (zframe_t *) zlist_pop (workers);
        zmsg_wrap (msg, identity);
        zframe_print (identity, "Broker: route req to worker ");
        zmsg_dump (msg);

        // Msg format: [WORKER ID] -> [] -> [CLIENT ID] -> [] -> [REQ DATA]
        zmsg_send (&msg, backend);
      }
    }
  }

  // When we’re done, clean up properly
  while (zlist_size (workers)) {
    frame = (zframe_t *) zlist_pop (workers);
    zframe_destroy (&frame);
  }
  zlist_destroy (&workers);

  // Notify discovery of shutdown
  printf("Broker: notifies discovery of shut down\n");
  msg = zmsg_new ();
  frame = zframe_new (DISC_DELETE, strlen (DISC_DELETE));
  zmsg_append (msg, &frame);
  strcpy (str, TCP_URL);
  strcat (str, argv[1]);
  strcat (str, ":");
  strcat (str, argv[3]);
  frame = zframe_new (str, strlen (str));
  zmsg_append (msg, &frame);
  zmsg_pushmem (msg, NULL, 0);
  zmsg_send (&msg, disc);

  zsocket_destroy (ctx, frontend);
  zsocket_destroy (ctx, backend);
  zsocket_destroy (ctx, disc);
  zctx_destroy (&ctx);
  return 0;
}
