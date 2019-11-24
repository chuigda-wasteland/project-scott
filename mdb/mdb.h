#ifndef MINIDB_H
#define MINIDB_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

enum {
  KEY_SIZE_MAX_LIMIT = UINT8_MAX - 1,
  VALUE_SIZE_MAX_LIMIT = UINT32_MAX - 1,
  HASH_BUCKETS_MAX = UINT32_MAX,
  ITEMS_MAX_LIMIT = UINT32_MAX,
  DB_NAME_MAX = UINT8_MAX
};

typedef struct {
  char *db_name;

  uint16_t key_size_max;
  uint32_t data_size_max;
  uint32_t hash_buckets;
  uint32_t items_max;
} mdb_options_t;

enum {
  MDB_OK = 0,
  MDB_NO_KEY,
  MDB_ERR_CRITICAL,
  MDB_ERR_LOGIC,
  MDB_ERR_OPEN_FILE,
  MDB_ERR_READ,
  MDB_ERR_ALLOC,
  MDB_ERR_SEEK,
  MDB_ERR_BUFSIZ,
  MDB_ERR_KEY_SIZE,
  MDB_ERR_VALUE_SIZE,
  MDB_ERR_UNIMPLEMENTED = 100
};

typedef struct {
  uint8_t code;
  const char *desc;
} mdb_status_t;

#define STAT_CHECK_RET(status, cleanup_code) \
  { if ((status).code != MDB_OK) { \
    cleanup_code \
    return status; \
  } }

typedef void *mdb_t;

mdb_status_t mdb_open(mdb_t *handle, const char *db_path);
mdb_status_t mdb_create(mdb_t *handle, mdb_options_t options);
mdb_status_t mdb_read(mdb_t handle, const char *key, char *buf, size_t bufsiz);
mdb_status_t mdb_write(mdb_t handle, const char *key, const char *value);
mdb_status_t mdb_delete(mdb_t handle, const char *key);
mdb_options_t mdb_get_options(mdb_t handle);

#endif // MINIDB_H
