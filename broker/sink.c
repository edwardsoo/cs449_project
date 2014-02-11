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
  INSERT,
  DELETE,
  FIND,
  RANGE
};


int main (void)
{
  int rv;  // Return Value/Count
  char str [BUFFSIZE], msg[BUFFSIZE];
  int successType;
  int duplicateTrue;
  int graphI;
  int graphJ;
  int graphI2;
  int graphJ2;
  double payload;
  int pid;

  void *context = zmq_ctx_new ();
  void *to_worker = zmq_socket (context, ZMQ_PUSH);
  void *pull_socket = zmq_socket (context, ZMQ_PULL);
  rv = zmq_bind (to_worker, "tcp://127.0.0.1:9990");
  assert (rv == 0);
  rv = zmq_bind (pull_socket, "tcp://127.0.0.1:5556");
  assert (rv == 0);

  long counter = 0;

  while (TRUE)
  {
    rv = zmq_recv (pull_socket, str, BUFFSIZE, 0);
    {
      str[rv] = '\0';

      if( !strcmp(str,"INSERT") )
      {
        zmq_recv (pull_socket, &successType, sizeof(int), 0);
        zmq_recv (pull_socket, &duplicateTrue, sizeof(int), 0);
        zmq_recv (pull_socket, &graphI, sizeof(int), 0);
        zmq_recv (pull_socket, &graphJ, sizeof(int), 0);
        zmq_recv (pull_socket, &payload, sizeof(double), 0);
        zmq_recv (pull_socket, &pid, sizeof(int), 0);

        printf("Received an INSERT reply [%10ld] with success=%d, duplicate=%d, i=%d, j=%d, w=%lf, lastPID=%d\n",
            counter++,successType,duplicateTrue,graphI,graphJ,payload,pid);

        sprintf(msg,
            "INSERT success=%d, duplicate=%d, i=%d, j=%d, w=%lf, last_PID=%d",
            successType, duplicateTrue, graphI, graphJ, payload, pid);
        zmq_send(to_worker, msg, strlen(msg), 0);
      }
      else if( !strcmp(str,"DELETE") )
      {
        zmq_recv (pull_socket, &successType, sizeof(int), 0);
        zmq_recv (pull_socket, &graphI, sizeof(int), 0);
        zmq_recv (pull_socket, &graphJ, sizeof(int), 0);
        zmq_recv (pull_socket, &pid, sizeof(int), 0);

        printf("Received an DELETE reply [%10ld] with success=%d, i=%d, j=%d, lastPID=%d\n",
            counter++,successType,graphI,graphJ,pid);

        sprintf(msg,
            "DELETE success=%d, duplicate=%d, i=%d, j=%d, last_PID=%d",
            successType, duplicateTrue, graphI, graphJ, pid);
        zmq_send(to_worker, msg, strlen(msg), 0);
      }
      else if( !strcmp(str,"FIND") )
      {
        zmq_recv (pull_socket, &successType, sizeof(int), 0);
        zmq_recv (pull_socket, &graphI, sizeof(int), 0);
        zmq_recv (pull_socket, &graphJ, sizeof(int), 0);
        zmq_recv (pull_socket, &payload, sizeof(double), 0);
        zmq_recv (pull_socket, &pid, sizeof(int), 0);

        printf("Received an FIND   reply [%10ld] with success=%d, i=%d, j=%d, w=%lf, lastPID=%d\n",
            counter++,successType,graphI,graphJ,payload,pid);

        sprintf(msg,
            "FIND success=%d, duplicate=%d, i=%d, j=%d, last_PID=%d",
            successType, duplicateTrue, graphI, graphJ, pid);
        zmq_send(to_worker, msg, strlen(msg), 0);
      }
      else if( !strcmp(str,"RANGE") )
      {
        zmq_recv (pull_socket, &graphI, sizeof(int), 0);
        zmq_recv (pull_socket, &graphJ, sizeof(int), 0);
        zmq_recv (pull_socket, &graphI2, sizeof(int), 0);
        zmq_recv (pull_socket, &graphJ2, sizeof(int), 0);
        zmq_recv (pull_socket, &payload, sizeof(double), 0);
        zmq_recv (pull_socket, &pid, sizeof(int), 0);

        printf("Received an RANGE  reply [%10ld] with i1=%d, j1=%d, i2=%d, j2=%d, weightSUM=%lf, lastPID=%3d\n",
            counter++,graphI,graphJ,graphI2,graphJ2,payload,pid);

        sprintf(msg,
            "RANGE i1=%d, j1=%d, i2=%d, j2=%d, weight_sum=%lf, last_PID=%d",
            graphI,graphJ,graphI2,graphJ2,payload,pid);
        zmq_send(to_worker, msg, strlen(msg), 0);
      }
      else
      {
        printf("Problem ... Error        [%10ld] \n",counter++);
      }
    }
  }

  zmq_close (to_worker);
  zmq_close (pull_socket);
  zmq_ctx_destroy (context);

  return 0;
}
