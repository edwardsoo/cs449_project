#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <signal.h>
#define TCP_URL "tcp://"

int main (int argc, char* argv[]) {
  zctx_t *ctx;
  zframe_t *frame;
  zmsg_t *req, *rep;
  int rv, success;
  char str[0x100], *key, *value, *op;

  if (argc != 2) {
    printf("Usage: %s PORT\n", argv[0]);
    exit(1);
  }

  ctx = zctx_new ();
  void *fe = zsocket_new (ctx, ZMQ_REP);

  strcpy (str, TCP_URL "*:");
  strcat (str, argv[1]);
  printf ("Name server: binds to %s\n", str);
  zsocket_bind (fe, str);

  zhash_t *table = zhash_new ();

  while (1) {
    req = zmsg_recv (fe);
    if (!req) {
      break;
    }

    // Operation type
    frame = zmsg_pop (req);
    op = zframe_strdup (frame);
    zframe_destroy (&frame);

    // Reply format: [SUCCESS] => [MSG] => [VALUE]
    rep = zmsg_new ();

    if (strcmp (op, "NAME_INSERT") == 0 && zmsg_size (req) == 2) {
      frame = zmsg_pop (req);
      key = zframe_strdup (frame);
      zframe_destroy (&frame);

      frame = zmsg_pop (req);
      value = zframe_strdup (frame);
      zframe_destroy (&frame);

      rv = zhash_insert (table, key, value);
      if (rv != 0) {
        success = 0;
        zmsg_addstr (rep, "duplicate key");

      } else {
        printf ("Name server: insert %s => %s\n", key, value);
        success = 1;
        zmsg_addstr (rep, "OK");
        zhash_freefn (table, key, free);
      }
      zmsg_addstr (rep, key);

    } else if (strcmp (op, "NAME_LOOKUP") == 0 && zmsg_size (req) == 1) {
      frame = zmsg_pop (req);
      key = zframe_strdup (frame);
      zframe_destroy (&frame);

      value = zhash_lookup (table, key);
      if (!value) {
        success = 0;
        zmsg_addstr (rep, "undefined");
        zmsg_addstr (rep, "");

      } else {
        success = 1;
        zmsg_addstr (rep, "found");
        zmsg_addstr (rep, value);
      }
      
    } else {
      success = 0;
      zmsg_addstr (rep, "bad command");
      zmsg_addstr (rep, "");
    }

    free (op);
    zmsg_destroy (&req);

    zmsg_pushmem (rep, &success, sizeof (int));
    zmsg_send (&rep, fe);
  }

  zhash_destroy (&table);
  zctx_destroy (&ctx);
  return 0;
}
