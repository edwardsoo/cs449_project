#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <broker.h>

int main (int argc, char *argv[]) {
  char str[100], *addr;
  zctx_t *ctx = zctx_new ();
  zmsg_t *msg;
  zframe_t* frame;

  if (argc < 2) {
    printf ("Usage: %s DISCOVERY_ADDR:PORT\n", argv[0]);
    exit(1);
  }

  void *disc = zsocket_new (ctx, ZMQ_DEALER);
  void *broker = zsocket_new (ctx, ZMQ_DEALER);
  strcpy (str, "tcp://");
  strcat (str, argv[1]);
  zsocket_connect (disc, str);

  // Request for a broker
  msg = zmsg_new ();
  zmsg_pushstr (msg, DISC_GET);
  zmsg_pushmem (msg, NULL, 0);
  zmsg_send (&msg, disc);

  // Receive broker address
  msg = zmsg_recv (disc);
  printf ("Received reply:\n");
  zmsg_dump (msg);
  frame = zmsg_last (msg);
  addr = zframe_strdup (frame);
  zmsg_destroy (&msg);
  printf ("Connects to broker %s\n", addr);
  zsocket_connect (broker, addr);
  free (addr);

  if (argc > 2) {
    // Send ready to broker
    printf ("Sends READY to broker\n");
    msg = zmsg_new ();
    zmsg_pushstr (msg, WORKER_READY);
    zmsg_pushmem (msg, 0, 0);
    zmsg_dump (msg);
    zmsg_send (&msg, broker);

    // Wait for broker to terminate
    printf ("Waiting for broker to terminate\n");
    msg = zmsg_recv (disc);
    printf ("Received msg:\n");
    zmsg_dump (msg);
    zmsg_destroy (&msg);

  } else {
    // Release broker
    msg = zmsg_new ();
    zmsg_pushstr (msg, DISC_PUT);
    zmsg_pushmem (msg, NULL, 0);
    zmsg_send (&msg, disc);

    // Wait for ack
    msg = zmsg_recv (disc);
    printf ("Received msg:\n");
    zmsg_dump (msg);
    zmsg_destroy (&msg);
  }
  return 0;
}
