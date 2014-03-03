#include <zmq.h>
#include <czmq.h>
#include <stdio.h>

typedef struct node_t {
  char * addr;
  int conn_avail;
} node;

void debug_node(node* n) {
  printf("node %s %d\n", n->addr, n->conn_avail);
}

int main (void) {
  zmsg_t *msg;
  zframe_t *frame;
  node *n;

  zctx_t *ctx = zctx_new();
  void *pull = zsocket_new(ctx, ZMQ_PULL);
  void *fe_rep = zsocket_new(ctx, ZMQ_REP); 
  void *be_rep = zsocket_new(ctx, ZMQ_REP); 

  zsocket_bind (pull, "tcp://*:11110");
  zsocket_bind (fe_rep, "tcp://*:11111");
  zsocket_bind (be_rep, "tcp://*:11112");

  zlist_t *fe_list = zlist_new();
  zlist_t *be_list = zlist_new();

  zmq_pollitem_t items [] = {
    {pull, 0, ZMQ_POLLIN, 0},
    {fe_rep, 0, ZMQ_POLLIN, 0},
    {be_rep, 0, ZMQ_POLLIN, 0}
  };
  
  while (1) {
    int rc = zmq_poll (items, (zlist_size(fe_list) || zlist_size(be_list))? 3: 1, -1);

    if (rc == -1)
      break;

    if (items [0].revents & ZMQ_POLLIN) {
      msg = zmsg_recv (pull);
      if (!msg)
        break;

      printf("Received from broker:\n");
      zmsg_dump(msg);

      // First frame: broker frontend address
      frame = zmsg_first(msg);
      n = malloc(sizeof(node));
      n->addr = zframe_strdup(frame);

      // Second frame: max number of frontend connection
      frame = zmsg_next(msg);
      memcpy(&(n->conn_avail), zframe_data(frame), sizeof(int));
      zlist_append(fe_list, n);

      // Third frame: broker backend address
      frame = zmsg_next(msg);
      n = malloc(sizeof(node));
      n->addr = zframe_strdup(frame);

      // Forth frame: max number of backend connection
      frame = zmsg_next(msg);
      memcpy(&(n->conn_avail), zframe_data(frame), sizeof(int));
      zlist_append(be_list, n);
    }

    if (zlist_size(fe_list) && items[1].revents & ZMQ_POLLIN) {
      msg = zmsg_recv(fe_rep);
      if (!msg)
        break;

      printf("Received request for broker frontend:\n");
      zmsg_dump(msg);
      n = (node*) zlist_first(fe_list);
      debug_node(n);
      zmq_send(fe_rep, n->addr, strlen(n->addr), 0);

      n->conn_avail--;
      if (n->conn_avail <= 0) {
        zlist_remove(fe_list, n);
        free(n);
      }
    }

    if (zlist_size(be_list) && items[2].revents & ZMQ_POLLIN) {
      msg = zmsg_recv(be_rep);
      if (!msg)
        break;

      printf("Received request for broker backend:\n");
      zmsg_dump(msg);
      n = (node*) zlist_first(be_list);
      debug_node(n);
      zmq_send(be_rep, n->addr, strlen(n->addr), 0);

      n->conn_avail--;
      if (n->conn_avail <= 0) {
        zlist_remove(be_list, n);
        free(n);
      }
    }
  }

  while (zlist_size (fe_list)) {
    n = (node*) zlist_pop(fe_list);
    free(n);
  }
  while (zlist_size (be_list)) {
    n = (node*) zlist_pop(be_list);
    free(n);
  }
  zlist_destroy (&fe_list);
  zlist_destroy (&be_list);
  zctx_destroy (&ctx);
  return 0;
}
