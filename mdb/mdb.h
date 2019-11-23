#ifndef MINIDB_H
#define MINIDB_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define KEY_SIZE_MAX_LIMIT   UINT8_MAX
#define VALUE_SIZE_MAX_LIMIT UINT32_MAX
#define HASH_BUCKETS_MAX     UINT32_MAX
#define ITEMS_MAX_LIMIT      UINT32_MAX
#define DB_NAME_MAX          UINT8_MAX

typedef struct {
  char *db_name;

  uint16_t key_size_max;
  uint32_t data_size_max;
  uint32_t hash_buckets;
  uint32_t items_max;
} mdb_options_t;

enum {
  MDB_OK = 0,
  MDB_ERR_CRITICAL = 1,
  MDB_ERR_LOGIC = 2,
  MDB_ERR_OPEN_FILE = 3,
  MDB_ERR_READ = 4,
  MDB_ERR_ALLOC = 5,
  MDB_ERR_SEEK = 6,
  MDB_ERR_UNIMPLEMENTED = 100
};

typedef struct {
  uint8_t code;
  const char *desc;
} mdb_status_t;

typedef void *mdb_t;

mdb_status_t mdb_open(mdb_t *handle, const char *db_path);
mdb_status_t mdb_create(mdb_t *handle, mdb_options_t options);
mdb_status_t mdb_read(mdb_t handle, const char *key, char *buf, size_t bufsiz);
mdb_status_t mdb_write(mdb_t handle, const char *key, const char *buf);
mdb_status_t mdb_delete(mdb_t handle, const char *key);
mdb_options_t mdb_get_options(mdb_t handle);

#endif // MINIDB_H
