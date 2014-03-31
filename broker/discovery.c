#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include "broker.h"

#define STR_LEN 0x101

void send_get_response (void*, zhash_t*, zframe_t*, char*);

// Backend address, peer-publish address and list of backends connected
typedef struct broker {
  zframe_t *id;
  char *be_addr;
  char *pub_addr;
  zlist_t *backends;
} broker_t;

void broker_free (void *data) {
  broker_t *broker = (broker_t*) data;

  while (zlist_size (broker->backends)) {
    zframe_destroy (zlist_pop (broker->backends));
  }
  free (broker->be_addr);
  free (broker->pub_addr);
  zframe_destroy (&(broker->id));
  free (broker);
}

int find_least_used_broker (const char *key, void *item, void *arg) {
  broker_t* broker = (broker_t*) item;
  broker_t** lu_broker = (broker_t**) arg;

  if ((*lu_broker) == NULL ||
      zlist_size((*lu_broker)->backends) > zlist_size (broker->backends)) {
    *lu_broker = broker;
  }
  return 0;
}

int zmsg_push_broker_pub_addr (const char *key, void *item, void *arg) {
  broker_t *broker = (broker_t*) item;
  zmsg_t *msg = (zmsg_t*) arg;
  zmsg_pushstr (msg, broker->pub_addr);
  return 0;
}

int zlist_push_broker_id (const char *key, void *item, void *arg) {
  broker_t *broker = (broker_t*) item;
  zlist_t *ids = (zlist_t*) arg;
  zlist_push (ids, zframe_dup (broker->id));
  return 0;
}

int main (int argc, char* argv[]) {
  zctx_t *ctx;
  zmsg_t *msg, *be_msg;
  zframe_t *identity, *frame, *op, *pend_back;
  broker_t *broker, *lu_broker;
  char str[STR_LEN], *idstr, *be_addr;
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

  // Maps broker address to a broker_t struct
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
      // Msg: ["ADD"] -> [Broker BE Address] -> [Broker Peer-Pub Address] -> NULL
      broker = malloc (sizeof (broker_t));
      broker->backends = zlist_new ();
      broker->id = zframe_dup (identity);
      frame = zmsg_pop (msg);
      broker->be_addr = zframe_strdup (frame);
      zframe_destroy (&frame);
      frame = zmsg_pop (msg);
      broker->pub_addr = zframe_strdup (frame);

      // Tell all existing brokers to subscribe to new broker
      zlist_t *ids = zlist_new ();
      zhash_foreach (broker_table, zlist_push_broker_id, ids);
      while (zlist_size (ids)) {
        zmsg_t* peer = zmsg_new ();
        zmsg_pushstr (peer, broker->pub_addr);
        zmsg_wrap (peer, zlist_pop (ids));
        zmsg_send (&peer, sock);
      }
      zlist_destroy (&ids);

      // Tell bew broker to subscribe to all existing brokers
      zmsg_t *peers = zmsg_new ();
      zhash_foreach (broker_table, zmsg_push_broker_pub_addr, peers);
      zmsg_wrap (peers, zframe_dup (identity));
      zmsg_send (&peers, sock);

      // Insert new broker into broker table
      rv = zhash_insert (broker_table, broker->be_addr, broker);
      if (rv == -1) {
        printf ("\nDiscovery: broker address %s already in table\n", broker->be_addr);
        broker_free (broker);

      } else {
        zhash_freefn (broker_table, broker->be_addr, broker_free);

        // If there are backend waiting for a broker, reply now
        if (zlist_size (pend_backends)) {
          printf ("\nDiscovery: replies to pending backend\n");
          while (zlist_size (pend_backends)) {
            pend_back = zlist_pop (pend_backends);
            zframe_print(pend_back, "pending backend:");
            send_get_response (sock, backend_table, pend_back, broker->be_addr);
            zlist_append (broker->backends, pend_back);
          }
        }
      }

      zframe_destroy (&frame);

    } else if (zframe_streq (op, DISC_DELETE)) {
      // Msg: ["DELETE"] -> [Broker Address] -> NULL
      frame = zmsg_pop (msg);
      be_addr = zframe_strdup (frame);
      broker = (broker_t*) zhash_lookup (broker_table, be_addr);
      if (broker) {
        while (zlist_size (broker->backends)) {
          
          be_msg = zmsg_new ();
          zmsg_pushstr (be_msg, DISC_STOP);
          zmsg_wrap (be_msg, zlist_pop (broker->backends));
          printf ("\nDiscovery: notifying backend of broker termination\n");
          zmsg_dump (be_msg);
          zmsg_send (&be_msg, sock);
          zhash_delete (backend_table, idstr);

        }
        zhash_delete (broker_table, be_addr);

      } else {
        printf ("\nDiscovery: cannot find broker address %s in table\n", be_addr);
      }
      free (be_addr);
      zframe_destroy (&frame);

    } else if (zframe_streq (op, DISC_PUT)) {
      // Msg: ["PUT"] -> NULL
      be_addr = zhash_lookup (backend_table, idstr);
      if (be_addr) {
        printf ("\nDiscovery: found broker address %s for backend ID %s\n",
          be_addr, idstr);

        broker = (broker_t*) zhash_lookup (broker_table, be_addr);

        if (!broker) {
          printf ("\nDiscovery: cannot find broker address %s in table\n", be_addr);

        } else {
          // Find the frame with same ID on list and remove it
          frame = (zframe_t*) zlist_first (broker->backends);
          while (frame) {
            if (zframe_eq (frame, identity)) {
              zlist_remove (broker->backends, frame);
              zframe_destroy (&frame);
              break;
            }
            frame = zlist_next (broker->backends);
          }
        }
        zhash_delete (backend_table, idstr);

        // Send ack
        be_msg = zmsg_new ();
        zmsg_pushstr (be_msg, DISC_OK);
        frame = zframe_dup (identity);
        zmsg_wrap (be_msg, frame);
        zmsg_send (&be_msg, sock);

      } else {
        printf ("\nDiscovery: cannot find socket ID %s in table\n", idstr);
      }

    } else if (zframe_streq (op, DISC_GET)) {
      // Msg: ["GET"] -> NULL

      if (zhash_size (broker_table)) {
        // Find the broker with least number of backend connection
        lu_broker = NULL;
        zhash_foreach (broker_table, find_least_used_broker, &lu_broker);
        printf ("Least used broker: address %s, %ld connections\n",
            lu_broker->be_addr, zlist_size (lu_broker->backends));

        send_get_response (sock, backend_table, identity, lu_broker->be_addr);
        zlist_append (lu_broker->backends, zframe_dup (identity));

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

void send_get_response (
    void* sock, zhash_t *backends, zframe_t* identity, char *addrstr
    ) {

  zframe_t *frame;
  int rv;
  char *idstr = zframe_strhex (identity);
  char *addr = malloc (strlen(addrstr) + 1);

  strcpy (addr, addrstr);

  // Update backend table
  printf ("\nDiscovery: inserts [%s]->[%s] into backend table\n", idstr, addr);
  rv = zhash_insert (backends, idstr, addr);
  if (rv == -1) {
    printf ("\nDiscovery: backend ID %s already in table\n", idstr);
  }
  zhash_freefn (backends, idstr, free);
  free (idstr);

  zmsg_t *be_msg = zmsg_new ();
  zmsg_pushstr (be_msg, addr);
  frame = zframe_dup (identity);
  zmsg_wrap (be_msg, frame);
  printf ("\nDiscovery: replies to backend\n");
  zmsg_dump (be_msg);
  zmsg_send (&be_msg, sock);

}
