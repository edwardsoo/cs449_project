#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <broker.h>

int main (int argc, char *argv[]) {
  char str[100];
  zctx_t *ctx = zctx_new ();
  zmsg_t *msg;

  if (argc < 2) {
    printf ("Usage: %s DISCOVERY_ADDR:PORT\n", argv[0]);
    exit(1);
  }

  void *disc = zsocket_new (ctx, ZMQ_DEALER);
  strcpy (str, "tcp://");
  strcat (str, argv[1]);
  zsocket_connect(disc, str);

  // Request for a broker
  msg = zmsg_new ();
  zmsg_pushstr (msg, DISC_GET);
  zmsg_pushmem (msg, NULL, 0);
  zmsg_send (&msg, disc);

  // Receive broker address
  msg = zmsg_recv (disc);
  printf ("Received reply:\n");
  zmsg_dump (msg);
  zmsg_destroy (&msg);

  if (argc > 2) {
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
