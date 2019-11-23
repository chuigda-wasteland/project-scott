#include "mdb.h"

#include <stdio.h>
#include <stdlib.h>

typedef struct {
  const char *db_name;

  FILE *fp_superblock;
  FILE *fp_index;
  FILE *fp_data;

  mdb_options_t options;
} mdb_int_t;

mdb_options_t mdb_options(mdb_t *handle) {
  mdb_int_t *db = (mdb_int_t*)(*handle);
  return db->options;
}
