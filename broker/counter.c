#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "counter.h"

list_t* list_new (char *key, int count, list_t *next) {
  list_t *self;
  self = malloc (sizeof (list_t));
  self->next = next;
  self->key = strdup (key);
  self->count = count;
  return self;
}

void list_destroy (list_t **list_p) {
  list_t *list;
  list = *list_p;
  free (list->key);
  if (list->next) {
    list_destroy (&(list->next));
  }
  free (list);
  *list_p = NULL;
}

counter_t* counter_new (int size) {
  counter_t* self;
  if (!size)
    return NULL;

  self = malloc (sizeof (counter_t));
  if (self) {
    self->array = malloc (sizeof (list_t*) * size);
    if (!self->array) {
      free (self);
      return NULL;
    }
    memset (self->array, 0, sizeof (list_t*) * size);
    self->size = size;
  }
  return self;
}

void counter_destroy (counter_t **counter_p) {
  int i;
  counter_t *counter = *counter_p;

  for (i = 0; i < counter->size; i++) {
    if (!counter->array[i]) continue;
    list_destroy (counter->array + i);
  }

  free (counter->array);
  free (counter);
  *counter_p = NULL;
}

uint32_t hash_code (char *key) {
  uint32_t h, i, key_len;
  key_len = strlen (key);
  h = 0;
  for (i = 0; i < key_len; i++) {
    h = (31 * h) + key[i];
  }
  return h;
}

/* Insert key-count into counter.
 * If key is alreay in counter, update count value
 */
void counter_insert (counter_t *self, char *key, int count) {
  uint32_t h;
  int cmp;
  list_t **list_p, *list;

  h = hash_code (key) % self->size;
  list_p = self->array + h;

  // Keep entries sorted by key in collision list
  while (*list_p) {
    list = *list_p;
    cmp = strcmp (key, list->key);

    if (cmp > 0) {
      // Did not find key, insert here
      *list_p = list_new (key, count, list);
      return;

    } else if (cmp == 0) {
      // Found key, update value
      list->count = count;
      return;
    }

    list_p = &(list->next);
  }
  *list_p = list_new (key, count, NULL);
}

/* Delete key-count from counter.
 * If key not found in counter then the counter is not modified
 */
void counter_delete (counter_t *self, char *key) {
  uint32_t h;
  int cmp;
  list_t **list_p, *list;

  h = hash_code (key) % self->size;
  list_p = self->array + h;

  while (*list_p) {
    list = *list_p;
    cmp = strcmp (key, list->key);

    if (cmp > 0) {
      // Did not find key, do nothing
      return;

    } else if (cmp == 0) {
      *list_p = list->next;
      list->next = NULL;
      list_destroy (&list);
      return;
    }

    list_p = &(list->next);
  }
}

/* Return the count value mapped by a key in the counter.
 * return 0 if key not found
 */
int counter_count (counter_t *self, char *key) {
  uint32_t h;
  int cmp;
  list_t **list_p, *list;

  h = hash_code (key) % self->size;
  list_p = self->array + h;

  while (*list_p) {
    list = *list_p;
    cmp = strcmp (key, list->key);

    if (cmp > 0) {
      // Did not find key
      return 0;

    } else if (cmp == 0) {
      // Found key
      return list->count;
    }

    list_p = &(list->next);
  }
  return 0;
}

/* Return a copy of the key with the maximum count */
char* counter_max (counter_t *self) {
  int i, max_count;
  char *max_key;
  list_t* list;

  max_count = INT_MIN;
  max_key = NULL;

  for (i = 0; i < self->size; i++) {
    if (!self->array[i]) continue;
    list = self->array[i];
    while (list) {
      if (list->count > max_count) {
        max_count = list->count;
        max_key = list->key;
      }
      list = list->next;
    }
  }

  if (max_key) {
    max_key = strdup (max_key);
  }
  return max_key;
}

/* Return sum of all counts */
int counter_sum (counter_t *self) {
  int i, sum;
  list_t* list;

  sum = 0;

  for (i = 0; i < self->size; i++) {
    if (!self->array[i]) continue;
    list = self->array[i];
    while (list) {
      sum += list->count;
      list = list->next;
    }
  }

  return sum;
}
