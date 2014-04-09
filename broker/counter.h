#ifndef COUNTER_H
#define COUNTER_H

typedef struct list {
  char* key;
  int count;
  struct list *next;
} list_t;

typedef struct counter {
  list_t** array;
  int size;
  int count;
} counter_t;

list_t* list_new (char *key, int count, list_t *next);
void list_destroy (list_t **list_p);

counter_t* counter_new (int size);
void counter_destroy (counter_t **counter_p);
void counter_insert (counter_t *self, char *key, int count);
void counter_delete (counter_t *self, char *key);
int counter_count (counter_t *self, char *key);
char* counter_max (counter_t *self);
int counter_sum (counter_t *self);

#endif
