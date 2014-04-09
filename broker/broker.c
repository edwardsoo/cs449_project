#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <signal.h>
#include "broker.h"
#define TCP_URL "tcp://"

void zmsg_full_dump (zmsg_t *msg) {
  printf("--------------------------------------\n");
  zframe_t *frame = zmsg_first (msg);
  while (frame) {
    zframe_fprint (frame, "", stdout);
    frame = zmsg_next (msg);
  }
}

// Publish a successful INSERT/DELETE to both frontend and peer with 2 prefixes
void publish (zmsg_t* msg, void* fe_pub, void* peer_pub) {
  zmsg_t *msg2, *peer_msg1, *peer_msg2;
  zframe_t *op, *dep_lat, *dep_lng, *arr_lat, *arr_lng;
  char *op_str;

  op = zmsg_first (msg);
  op_str = zframe_strdup (op);

  if (zframe_streq (op, "INSERT")) {
    zmsg_next (msg);
  }
  zmsg_next (msg);
  dep_lat = zmsg_next (msg);
  dep_lng = zmsg_next (msg);
  zmsg_next (msg); // departure time
  arr_lat = zmsg_next (msg);
  arr_lng = zmsg_next (msg);

  msg2 = zmsg_dup (msg);
  zmsg_pushstr (msg, "(%ld,%ld)->(%ld,%ld)%s",
      *((long long*) zframe_data (dep_lat)),
      *((long long*) zframe_data (dep_lng)),
      *((long long*) zframe_data (arr_lat)),
      *((long long*) zframe_data (arr_lng)), op_str);
  zmsg_pushstr (msg2, "(%ld,%ld)",
      *((long long*) zframe_data (arr_lat)),
      *((long long*) zframe_data (arr_lng)));

  peer_msg1 = zmsg_dup (msg);
  peer_msg2 = zmsg_dup (msg2);
  zmsg_send (&msg, fe_pub);
  zmsg_send (&msg2, fe_pub);
  zmsg_send (&peer_msg1, peer_pub);
  zmsg_send (&peer_msg2, peer_pub);
  free (op_str);
}

int main (int argc, char* argv[])
{
  zctx_t *ctx;
  zframe_t *identity, *frame;
  zmsg_t *msg;
  char str[0x100], *ptr;

  if (argc != 7) {
    printf ("Usage: %s HOST_ADDR FRONTEND_PORT BACKEND_PORT FRONTEND_PUB_PORT PEER_PUB_PORT DISCOVERY_ADDR:PORT\n",
        argv[0]);
    printf ("Example: %s 1.2.3.4 9997 9996 9998 9999 5.6.4.7:11111\n", argv[0]);
    exit (1);
  }

  ctx = zctx_new ();
  void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
  void *backend = zsocket_new (ctx, ZMQ_ROUTER);
  void *fe_pub = zsocket_new (ctx, ZMQ_XPUB);
  void *peer_sub = zsocket_new (ctx, ZMQ_XSUB);
  void *peer_pub = zsocket_new (ctx, ZMQ_PUB);

  strcpy (str, TCP_URL "*:");
  strcat (str, argv[2]);
  printf ("Broker: binds frontend to %s\n", str);
  zsocket_bind (frontend, str);

  strcpy (str, TCP_URL "*:");
  strcat (str, argv[3]);
  printf ("Broker: binds backend to %s\n", str);
  zsocket_bind (backend, str);

  strcpy (str, TCP_URL "*:");
  strcat (str, argv[4]);
  printf ("Broker: binds client pub to %s\n", str);
  zsocket_bind (fe_pub, str);

  strcpy (str, TCP_URL "*:");
  strcat (str, argv[5]);
  printf ("Broker: binds peer pub to %s\n", str);
  zsocket_bind (peer_pub, str);

  // Notify broker discovery service of this broker instance
  void *disc = zsocket_new (ctx, ZMQ_DEALER);
  int linger = -1;
  zmq_setsockopt (disc, ZMQ_LINGER, &linger, sizeof (linger));
  strcpy (str, TCP_URL);
  strcat (str, argv[6]);
  zsocket_connect (disc, str);

  // Msg: [Backend address] -> [Peer pub address] -> []
  msg = zmsg_new ();
  frame = zframe_new (DISC_ADD, strlen (DISC_ADD));
  zmsg_append (msg, &frame);
  strcpy (str, TCP_URL);
  strcat (str, argv[1]);
  strcat (str, ":");
  strcat (str, argv[3]);
  frame = zframe_new (str, strlen (str));
  zmsg_append (msg, &frame);
  strcpy (str, TCP_URL);
  strcat (str, argv[1]);
  strcat (str, ":");
  strcat (str, argv[5]);
  frame = zframe_new (str, strlen (str));
  zmsg_append (msg, &frame);
  zmsg_pushmem (msg, NULL, 0);
  printf ("Broker: advertises self to discovery\n");
  zmsg_send (&msg, disc);

  // Wait for a list of peer pub addresses of all other brokers
  msg = zmsg_recv (disc);
  if (!msg)
    return 1;

  // Subscribe to all other brokers
  while ((ptr = zmsg_popstr (msg)) != NULL) {
    zsocket_connect (peer_sub, ptr);
    free (ptr);
  }
  
  // Queue of available workers
  zlist_t *workers = zlist_new ();
  
  zmq_pollitem_t items [] = {
    {peer_sub, 0, ZMQ_POLLIN, 0},
    {fe_pub, 0, ZMQ_POLLIN, 0},
    {backend, 0, ZMQ_POLLIN, 0},
    {disc, 0, ZMQ_POLLIN, 0},
    {frontend, 0, ZMQ_POLLIN, 0}
  };


  printf ("Broker: enters main loop\n");
  while (1) {
    // Poll frontend only if we have available workers
    int rc = zmq_poll (items, zlist_size (workers)? 5: 4, -1);

    // Interrupted
    if (rc == -1) {
      printf("Broker: poller interrupted\n");
      break;
    }

    if (items [0].revents & ZMQ_POLLIN) {
      // Got pub message from peer
      msg = zmsg_recv (peer_sub);
      if (!msg)
        break;

      // Publish to frontend pub socket
      zmsg_send (&msg, fe_pub);
    }

    if (items [1].revents & ZMQ_POLLIN) {
      // Got sub message from frontend
      msg = zmsg_recv (fe_pub);
      if (!msg)
        break;

      // Set subscription
      zmsg_send (&msg, peer_sub);
    }

    if (items [2].revents & ZMQ_POLLIN) {
      // Use worker identity for load-balancing
      // Msg format: [WORKER ID] -> [CLIENT ID] -> [] -> [...]
      msg = zmsg_recv (backend);

      // Interrupted
      if (!msg)
        break;

      // printf ("Broker: got message from worker\n");
      // zmsg_dump (msg);

      identity = zmsg_unwrap (msg);

      // Forward message to client if it’s not a READY
      frame = zmsg_first (msg);
      if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0) {
        // Save worker on available list
        zlist_append (workers, identity);

        // Msg format: [READY]
        zmsg_destroy (&msg);

      } else if (zframe_size (frame) == 0) {
        // No Client ID; meant to be published
        // Discard worker ID
        zframe_destroy (&identity);

        // Msg format: [] -> [REP]
        zmsg_remove (msg, frame);
        publish (msg, fe_pub, peer_pub);

      } else {
        // Save worker on available list
        zlist_append (workers, identity);

        // Msg format: [CLIENT ID] -> [] -> [REP]
        identity = zmsg_unwrap (msg);
        zframe_t *op, *success;
        op = zmsg_first (msg);
        success = zmsg_next (msg);
        int *success_val = (int*) zframe_data (success);

        if ((zframe_streq (op, "INSERT") || zframe_streq (op, "DELETE")) &&
            *success_val) {
          // Publish 2 msgs to both frontend and peer if success INSERT/DELETE
          publish (msg, fe_pub, peer_pub);
          zframe_destroy (&identity);

        } else if (zframe_streq (op, "FIND") || zframe_streq (op, "RANGE") ||
            !(*success_val)) {
          // Route back to client if FIND/RANGE or unsuccessful INSERT/DELETE
          zmsg_wrap (msg, identity);
          zmsg_send (&msg, frontend);
        }
      }
      printf ("Broker: %ld workers left\n", zlist_size (workers));
    }

    if (items [3].revents & ZMQ_POLLIN) {
      // Got message from discovery
      ptr = zstr_recv (disc);
      if (!ptr)
        break;

      // printf ("Broker: received new peer address %s\n", ptr);
      zsocket_connect (peer_sub, ptr);
      zsocket_set_subscribe (peer_sub, "INSERT");
      zsocket_set_subscribe (peer_sub, "DELETE");
      free (ptr);
    }

    if (items [4].revents & ZMQ_POLLIN) {
      // Got client request
      // Msg format: [CLIENT ID] -> [] -> [REQ]
      zmsg_t *msg = zmsg_recv (frontend);
      printf ("Broker: got request from client\n");
      // zmsg_dump (msg);

      if (!msg)
        break;

      // Route to first available worker
      identity = (zframe_t *) zlist_pop (workers);
      // printf ("Broker: %ld workers left\n", zlist_size (workers));

      zmsg_wrap (msg, identity);
      // zframe_print (identity, "Broker: route req to worker ");

      // Msg format: [WORKER ID] -> [] -> [CLIENT ID] -> [] -> [REQ DATA]
      zmsg_send (&msg, backend);
      printf ("Broker: %ld workers left\n", zlist_size (workers));
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
