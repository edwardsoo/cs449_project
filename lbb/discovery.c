#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include "broker.h"

#define STR_LEN 0x101

void get_response (void*, zhash_t*, zhash_t*, zframe_t*, char*);

typedef struct zhash_foreach_arg {
  char addr[STR_LEN];
  int num_conn;
} zhash_foreach_arg_t;

void free_broker_table_entry (void *data) {
  zlist_t *backends = (zlist_t*) data;
  while (zlist_size (backends)) {
    zframe_destroy (zlist_pop (backends));
  }
}

int find_least_used_broker (const char *key, void *item, void *argument) {
  zhash_foreach_arg_t *arg = (zhash_foreach_arg_t*) argument;
  zlist_t *backends = (zlist_t*) item;

  if (!strlen (arg->addr) || arg->num_conn > zlist_size (backends)) {
    strcpy (arg->addr, key);
    arg->num_conn = zlist_size (backends);
  }
  return 0;
}

int main (int argc, char* argv[]) {
  zctx_t *ctx;
  zmsg_t *msg, *be_msg;
  zframe_t *identity, *frame, *op, *pend_back;
  zlist_t *backends;
  char str[STR_LEN], *idstr, *addr;
  int rv;

  if (argc != 2) {
    printf ("Usage: %s PORT\n", argv[0]);
    exit (1);
  }

  ctx = zctx_new ();
  void *sock = zsocket_new (ctx, ZMQ_ROUTER);

  strcpy (str, "tcp://*:");
  strcat (str, argv[1]);
  printf ("\nDiscovery: binding to %s\n", str);
  zsocket_bind (sock, str);

  // Maps broker address to list of backends connected to broker
  zhash_t *broker_table = zhash_new ();
  // Maps backend ID (hex string) to broker address assigned
  zhash_t *backend_table = zhash_new ();
  // List of backend waiting for a broker
  zlist_t *pend_backends = zlist_new ();

  while (1) {
    msg = zmsg_recv (sock);
    if (!msg)
      break;

    printf ("\nDiscovery: received msg:\n");
    zmsg_dump (msg);

    identity = zmsg_unwrap(msg);
    idstr = zframe_strhex (identity);
    op = zmsg_pop(msg);

    if (zframe_streq (op , DISC_ADD)) {
      // Msg: ["ADD"] -> [Broker Address] -> NULL
      frame = zmsg_pop (msg);
      addr = zframe_strdup (frame);
      rv = zhash_insert (broker_table, addr, zlist_new ());
      if (rv == -1) {
        printf ("\nDiscovery: broker address %s already in table\n", addr);
      }
      zhash_freefn (broker_table, addr, free_broker_table_entry);

      // If there are backend waiting for a broker, reply now
      if (zlist_size (pend_backends)) {
        printf ("\nDiscovery: replies to pending backend\n");
        while (zlist_size (pend_backends)) {
          pend_back = zlist_pop (pend_backends);
          zframe_print(pend_back, "pending backend:");
          get_response (sock, broker_table, backend_table,
              pend_back, addr);
          zframe_destroy (&pend_back);
        }
      }

      free (addr);
      zframe_destroy (&frame);

    } else if (zframe_streq (op, DISC_DELETE)) {
      // Msg: ["DELETE"] -> [Broker Address] -> NULL
      frame = zmsg_pop (msg);
      addr = zframe_strdup (frame);
      backends  = (zlist_t*) zhash_lookup (broker_table, addr);
      if (backends) {
        while (zlist_size (backends)) {
          
          be_msg = zmsg_new ();
          zmsg_pushstr (be_msg, DISC_STOP);
          zmsg_wrap (be_msg, zlist_pop (backends));
          printf ("\nDiscovery: notifying backend of broker termination\n");
          zmsg_dump (be_msg);
          zmsg_send (&be_msg, sock);
          zhash_delete (backend_table, idstr);

        }
        zhash_delete (broker_table, addr);

      } else {
        printf ("\nDiscovery: cannot find broker address %s in table\n", addr);
      }
      free (addr);
      zframe_destroy (&frame);

    } else if (zframe_streq (op, DISC_PUT)) {
      // Msg: ["PUT"] -> NULL
      addr = zhash_lookup (backend_table, idstr);
      if (addr) {
        printf ("\nDiscovery: found broker address %s for backend ID %s\n",
          addr, idstr);

        backends = (zlist_t*) zhash_lookup (broker_table, addr);

        if (!backends) {
          printf ("\nDiscovery: cannot map broker address %s to list\n", addr);

        } else {
          // Find the frame with same ID on list and remove it
          frame = (zframe_t*) zlist_first (backends);
          while (frame) {
            if (zframe_eq (frame, identity)) {
              zlist_remove (backends, frame);
              zframe_destroy (&frame);
              break;
            }
            frame = zlist_next (backends);
          }
        }
        zhash_delete (backend_table, idstr);

        // Send ack
        be_msg = zmsg_new ();
        zmsg_pushstr (be_msg, DISC_OK);
        frame = zframe_dup (identity);
        zmsg_wrap (be_msg, identity);
        zmsg_send (&be_msg, sock);

      } else {
        printf ("\nDiscovery: cannot find socket ID %s in table\n", idstr);
      }

    } else if (zframe_streq (op, DISC_GET)) {
      // Msg: ["GET"] -> NULL

      if (zhash_size (broker_table)) {
        // Find the broker with least number of backend connection
        zhash_foreach_arg_t arg = {"", 0};
        zhash_foreach (broker_table, find_least_used_broker, &arg);
        printf ("Least used broker: address %s, %d connections\n",
            arg.addr, arg.num_conn);

        get_response (sock, broker_table, backend_table, identity, arg.addr);

      } else {
        // Put backend on waiting list
        frame = zframe_dup (identity);
        zlist_append (pend_backends, frame);
      }
    }

    free (idstr);
    zframe_destroy (&op);
    zframe_destroy (&identity);
    zmsg_destroy (&msg);
  }

  zhash_destroy (&broker_table);
  zhash_destroy (&backend_table);
  zctx_destroy (&ctx);
  return 0;
}

void get_response (
    void* sock, zhash_t* brokers, zhash_t *backends,
    zframe_t* identity, char *addrstr
    ) {
  int rv;
  char *idstr = zframe_strhex (identity);
  char *addr = malloc (strlen(addrstr) + 1);
  strcpy (addr, addrstr);

  zmsg_t *be_msg = zmsg_new ();
  zmsg_pushstr (be_msg, addr);
  zframe_t *frame = zframe_dup (identity);
  zmsg_wrap (be_msg, frame);
  printf ("\nDiscovery: replies to backend\n");
  zmsg_dump (be_msg);
  zmsg_send (&be_msg, sock);

  // Update broker table
  zlist_t *backend_list = zhash_lookup (brokers, addr);
  frame = zframe_dup (identity);
  zlist_append (backend_list, frame);

  // Update backend table
  printf ("\nDiscovery: inserts [%s]->[%s] into backend table\n", idstr, addr);
  rv = zhash_insert (backends, idstr, addr);
  if (rv == -1) {
    printf ("\nDiscovery: backend ID %s already in table\n", idstr);
  }
  zhash_freefn (backends, idstr, free);
  free (idstr);
}
