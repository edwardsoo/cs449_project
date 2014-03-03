#include <zmq.h>
#include <czmq.h>
#include <stdio.h>

int main (void) {
  char str[100];
  int num;
  zctx_t *ctx = zctx_new();

  void *broker_be_req = zsocket_new(ctx, ZMQ_REQ);
  zsocket_connect(broker_be_req, "tcp://localhost:11112");

  // Request msg does not matter
  zmq_send(broker_be_req, NULL, 0, 0);

  num = zmq_recv(broker_be_req, str, 100, 0);
  assert(num);
  str[num] = 0;
  printf("Discovered broker backend address %s\n", str);
  return 0;
}
