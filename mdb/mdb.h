#ifndef MINIDB_H
#define MINIDB_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define KEY_SIZE_MAX_LIMIT 256
#define VALUE_SIZE_MAX_LIMIT UINT32_MAX
#define ITEMS_MAX_LIMIT UINT32_MAX

typedef struct {
  const char *db_name;

  size_t key_size_max;
  size_t data_size_max;
  size_t hash_buckets;
  size_t items_max;
} mdb_options_t;

enum {
  MDB_OK = 0,
  MDB_CRITICAL = 1,
  MDB_LOGIC = 2,
};

typedef struct {
  uint8_t code;
  const char *description;
} mdb_status_t;

typedef void *mdb_t;

mdb_status_t mdb_open(mdb_t *handle, const char *db_path);
mdb_status_t mdb_create(mdb_t *handle, mdb_options_t options);
mdb_status_t mdb_read(mdb_t *handle, const char *key, char *buf, size_t bufsiz);
mdb_status_t mdb_write(mdb_t *handle, const char *key, const char *buf);
mdb_status_t mdb_delete(mdb_t *handle, const char *key);
mdb_options_t mdb_options(mdb_t *handle);

#endif // MINIDB_H
