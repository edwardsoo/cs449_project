/*
   <TYPE,Seq>                   <Return>
   <INSERT,i,j,w>              <INSERT,Status,Duplicate,i,j,w,pid>
   <DELETE,i,j>                <DELETE,Status,i,j,pid>
   <FIND,i,j>                  <FIND,Status,i,j,w,pid>
   <RANGE,i1,j1,i2,j2>         <RANGE,i1,j1,i2,j2,payload_sum,pid>
 */

#include "Python.h"
#include <zmq.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#define TRUE  1
#define FALSE 0

#define BUFFSIZE    100

enum msg_type
{
  INSERT = 0,
  DELETE,
  FIND,
  RANGE
};

int main (void)
{
  int i1, j1, i2, j2;
  double weight;
  int rv;  // Return Value/Count
  char str[BUFFSIZE];
  char op[BUFFSIZE];

  void *context = zmq_ctx_new ();
  void *push_socket = zmq_socket (context, ZMQ_PUSH);
  void *from_worker = zmq_socket (context, ZMQ_PULL);
  rv = zmq_bind (push_socket, "tcp://127.0.0.1:5555");
  assert (rv == 0);
  rv = zmq_bind (from_worker, "tcp://*:9991");
  assert (rv == 0);

  int more;
  size_t more_size = sizeof(more);

  while (1) {

    rv = zmq_recv (from_worker, op, BUFFSIZE, 0);

    if (!strcmp(op, "INSERT") || !strcmp(op, "DELETE") ||
        !strcmp(op, "FIND") || !strcmp(op, "RANGE")) {

      zmq_getsockopt(from_worker, ZMQ_RCVMORE, &more, &more_size);
      if (!more) {
        continue;
      }

      // Parse vertices
      zmq_recv (from_worker, str, BUFFSIZE, 0);
      i1 = atoi(str);
      zmq_recv (from_worker, str, BUFFSIZE, 0);
      j1 = atoi(str);


      if (!strcmp(op, "INSERT")) {
        // Parse weight
        zmq_recv (from_worker, str, BUFFSIZE, 0);
        weight = atof(str);

        printf("sending %s, %d, %d, %f\n", op, i1, j1, weight);
        zmq_send (push_socket, op,         strlen(op),    ZMQ_SNDMORE);
        zmq_send (push_socket, &i1,    sizeof(int),    ZMQ_SNDMORE);
        zmq_send (push_socket, &j1,    sizeof(int),    ZMQ_SNDMORE);
        zmq_send (push_socket, &weight, sizeof(double), 0);


      } else if (!strcmp(op, "DELETE") || !strcmp(op, "FIND")) {
        printf("sending %s, %d, %d\n", op, i1, j1);
        zmq_send(push_socket, op,         strlen(op),    ZMQ_SNDMORE);
        zmq_send(push_socket, &i1,    sizeof(int),    ZMQ_SNDMORE);
        zmq_send(push_socket, &j1,    sizeof(int),    0);

      } else if (!strcmp(op, "RANGE")) {
        // Parse vertices
        zmq_recv (from_worker, str, BUFFSIZE, 0);
        i2 = atoi(str);
        zmq_recv (from_worker, str, BUFFSIZE, 0);
        j2 = atoi(str);

        printf("sending %s, %d, %d, %d, %d\n", op, i1, j1, i2, j2);
        zmq_send(push_socket, op,         strlen(op),    ZMQ_SNDMORE);
        zmq_send(push_socket, &i1,    sizeof(int),    ZMQ_SNDMORE);
        zmq_send(push_socket, &j1,    sizeof(int),    ZMQ_SNDMORE);
        zmq_send(push_socket, &i2,    sizeof(int),    ZMQ_SNDMORE);
        zmq_send(push_socket, &j2,    sizeof(int),    0);
      }
    } 
  }

  zmq_close (push_socket);
  zmq_close (from_worker);
  zmq_ctx_destroy (context);

  return 0;
}
