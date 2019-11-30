#include "mdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

typedef uint32_t mdb_size_t;
typedef uint32_t mdb_ptr_t;

enum {
  MDB_PTR_SIZE = sizeof(mdb_ptr_t),
  MDB_DATALEN_SIZE = sizeof(mdb_size_t)
};

typedef struct {
  char *db_name;

  FILE *fp_superblock;
  FILE *fp_index;
  FILE *fp_data;

  mdb_options_t options;

  uint32_t index_record_size;
} mdb_int_t;

static mdb_status_t mdb_status(uint8_t code, const char *desc);
static mdb_int_t *mdb_alloc(void);
static void mdb_free(mdb_int_t *db);
static uint32_t mdb_hash(const char *key);

static mdb_status_t mdb_read_bucket(mdb_int_t *db, uint32_t bucket,
                                    mdb_ptr_t *ptr);
static mdb_status_t mdb_read_index(mdb_int_t *db, mdb_ptr_t idxptr,
                                   mdb_ptr_t *nextptr, char *keybuf,
                                   mdb_ptr_t *valptr, mdb_size_t *valsize);
static mdb_status_t mdb_read_data(mdb_int_t *db, mdb_ptr_t valptr,
                                  mdb_size_t valsize, char *valbuf,
                                  mdb_size_t bufsiz);
static mdb_status_t mdb_index_alloc(mdb_int_t *db, mdb_ptr_t *ptr);
static mdb_status_t mdb_data_alloc(mdb_int_t *db, mdb_size_t valsize,
                                   mdb_ptr_t *ptr);
static mdb_status_t mdb_index_free(mdb_int_t *db, mdb_ptr_t ptr);
static mdb_status_t mdb_data_free(mdb_int_t *db, mdb_ptr_t valptr,
                                  mdb_size_t valsize);

static char pathbuf[4096];

mdb_status_t mdb_open(mdb_t *handle, const char *path) {
  mdb_int_t *db = mdb_alloc();
  if (db == NULL) {
    return mdb_status(MDB_ERR_ALLOC,
                      "failed allocating memory buffer for database");
  }

  strcpy(pathbuf, path);
  strcat(pathbuf, ".db.super");
  db->fp_superblock = fopen(pathbuf, "r");
  if (db->fp_superblock == NULL) {
    mdb_free(db);
    return mdb_status(MDB_ERR_OPEN_FILE, "cannot open superblock file as read");
  }

  fscanf(db->fp_superblock, "%s", db->db_name);
  fscanf(db->fp_superblock, "%hu", &(db->options.key_size_max));
  fscanf(db->fp_superblock, "%u", &(db->options.data_size_max));
  fscanf(db->fp_superblock, "%u", &(db->options.hash_buckets));
  fscanf(db->fp_superblock, "%u", &(db->options.items_max));

  db->index_record_size =
      db->options.key_size_max
      + MDB_PTR_SIZE * 2
      + MDB_DATALEN_SIZE;

  if (ferror(db->fp_superblock)) {
    mdb_free(db);
    return mdb_status(MDB_ERR_READ, "read error when parsing superblock");
  }

  strcpy(pathbuf, path);
  strcat(pathbuf, ".db.index");
  db->fp_index = fopen(pathbuf, "r+");
  if (db->fp_index == NULL) {
    mdb_free(db);
    return mdb_status(MDB_ERR_OPEN_FILE, "cannot open index file as readwrite");
  }

  strcpy(pathbuf, path);
  strcat(pathbuf, ".db.data");
  db->fp_data = fopen(pathbuf, "r+");
  if (db->fp_index == NULL) {
    mdb_free(db);
    return mdb_status(MDB_ERR_OPEN_FILE, "cannot open data file as readwrite");
  }

  *handle = (mdb_t)db;
  return mdb_status(MDB_OK, NULL);
}

mdb_status_t mdb_read(mdb_t handle, const char *key, char *buf, size_t bufsiz) {
  mdb_int_t *db = (mdb_int_t*)handle;
  mdb_size_t bucket = mdb_hash(key) % db->options.hash_buckets;

  uint32_t ptr;
  mdb_status_t bucket_read_status = mdb_read_bucket(db, bucket, &ptr);
  STAT_CHECK_RET(bucket_read_status, {;});
  if (fseek(db->fp_index, (long)ptr, SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to index record");
  }

  mdb_ptr_t next_ptr;
  char *key_buffer = (char*)malloc(db->options.key_size_max + 1);
  mdb_ptr_t value_ptr;
  mdb_size_t value_size;

  mdb_status_t read_status = mdb_read_index(db, ptr, &next_ptr, key_buffer,
                                            &value_ptr, &value_size);
  STAT_CHECK_RET(read_status, { free(key_buffer); })

  while (strcpy(key_buffer, key) != 0 && ptr != 0) {
    read_status = mdb_read_index(db, ptr, &next_ptr, key_buffer,
                                 &value_ptr, &value_size);
    STAT_CHECK_RET(read_status, { free(key_buffer); })
    ptr = next_ptr;
  }

  if (next_ptr == 0) {
    free(key_buffer);
    return mdb_status(MDB_NO_KEY, "Key not found");
  }

  free(key_buffer);
  return mdb_read_data(db, value_ptr, value_size, buf, bufsiz);
}

mdb_status_t mdb_write(mdb_t handle, const char *key, const char *value) {
  mdb_int_t *db = (mdb_int_t*)handle;
  mdb_size_t bucket = mdb_hash(key) % db->options.hash_buckets;
  mdb_size_t new_key_size = strlen(key);
  if (new_key_size > db->options.key_size_max) {
    return mdb_status(MDB_ERR_KEY_SIZE, "key size too large");
  }
  mdb_size_t new_value_size = strlen(value);
  if (new_value_size > db->options.data_size_max) {
    return mdb_status(MDB_ERR_VALUE_SIZE, "value size too large");
  }

  uint32_t ptr;
  mdb_status_t bucket_read_status = mdb_read_bucket(db, bucket, &ptr);
  STAT_CHECK_RET(bucket_read_status, {;})
  if (fseek(db->fp_index, (long)ptr, SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to index record");
  }

  uint32_t next_ptr;
  char *key_buffer = (char*)malloc(db->options.key_size_max + 1);
  mdb_ptr_t value_ptr;
  mdb_size_t value_size;
  mdb_status_t read_status = mdb_read_index(db, ptr, &next_ptr, key_buffer,
                                            &value_ptr, &value_size);
  STAT_CHECK_RET(read_status, {free(key_buffer);})

  mdb_ptr_t save_ptr = ptr;
  ptr = next_ptr;
  while (strcpy(key_buffer, key) != 0 && ptr != 0) {
    read_status = mdb_read_index(db, ptr, &next_ptr, key_buffer,
                                 &value_ptr, &value_size);
    STAT_CHECK_RET(read_status, {free(key_buffer);})
    save_ptr = ptr;
    ptr = next_ptr;
  }
  free(key_buffer);

  if (ptr == 0) {
    mdb_ptr_t new_idx_ptr;
    mdb_status_t idx_alloc_status = mdb_index_alloc(db, &new_idx_ptr);
    STAT_CHECK_RET(idx_alloc_status, {;})

    mdb_ptr_t new_value_ptr;
    mdb_status_t data_alloc_status = mdb_data_alloc(db, new_value_size,
                                                    &new_value_ptr);
    STAT_CHECK_RET(data_alloc_status, {;})
  } else {
    mdb_status_t free_data_status = mdb_data_free(db, value_ptr, value_size);
    STAT_CHECK_RET(free_data_status, {;})
    mdb_status_t free_idx_status = mdb_index_free(db, save_ptr);
    STAT_CHECK_RET(free_idx_status, {;})

    mdb_ptr_t new_value_ptr;
    mdb_status_t data_alloc_status = mdb_data_alloc(db, new_value_size,
                                                    &new_value_ptr);
    STAT_CHECK_RET(data_alloc_status, {;})
  }

  return mdb_status(MDB_ERR_UNIMPLEMENTED, NULL);
}

mdb_options_t mdb_get_options(mdb_t handle) {
  mdb_int_t *db = (mdb_int_t*)handle;
  return db->options;
}

static mdb_status_t mdb_read_bucket(mdb_int_t *db, uint32_t bucket,
                                    mdb_ptr_t *ptr) {
  if (fseek(db->fp_index, (long)((bucket + 1) * MDB_PTR_SIZE), SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to bucket");
  }
  if (fread(&ptr, MDB_PTR_SIZE, 1, db->fp_index) != MDB_PTR_SIZE) {
    return mdb_status(MDB_ERR_READ, "cannot read bucket");
  }
  return mdb_status(MDB_OK, NULL);
}

static mdb_status_t mdb_read_index(mdb_int_t *db, mdb_ptr_t idxptr,
                                   mdb_ptr_t *nextptr, char *keybuf,
                                   mdb_ptr_t *valptr, mdb_size_t *valsize) {
  if (fseek(db->fp_index, (long)idxptr, SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to ptr");
  }
  if (fread(nextptr, MDB_PTR_SIZE, 1, db->fp_index) != MDB_PTR_SIZE) {
    return mdb_status(MDB_ERR_READ, "cannot read next ptr");
  }
  if (fread(keybuf, 1, db->options.key_size_max, db->fp_index)
      != db->options.key_size_max) {
    return mdb_status(MDB_ERR_READ, "cannot read key");
  }
  keybuf[db->options.key_size_max] = '\0';
  if (fread(valptr, MDB_PTR_SIZE, 1, db->fp_index) != MDB_PTR_SIZE) {
    return mdb_status(MDB_ERR_READ, "cannot read value ptr");
  }
  if (fread(valsize, MDB_DATALEN_SIZE, 1, db->fp_index)
      != MDB_DATALEN_SIZE) {
    return mdb_status(MDB_ERR_READ, "cannot read value length");
  }
  return mdb_status(MDB_OK, NULL);
}

static mdb_status_t mdb_read_data(mdb_int_t *db, mdb_ptr_t valptr,
                                  mdb_size_t valsize, char *valbuf,
                                  mdb_size_t bufsiz) {
  if (bufsiz < valsize + 1) {
    return mdb_status(MDB_ERR_BUFSIZ, "value buffer size too small");
  }
  if (fseek(db->fp_data, (long)valptr, SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to value");
  }
  if (fread(valbuf, 1, valsize, db->fp_data) != valsize) {
    return mdb_status(MDB_ERR_READ, "cannot read data");
  }
  valbuf[valsize] = '\0';
  return mdb_status(MDB_OK, NULL);
}

static mdb_status_t mdb_index_alloc(mdb_int_t *db, mdb_ptr_t *ptr) {
  if (fseek(db->fp_index, 0, SEEK_SET) < 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to head of index file");
  }
  mdb_ptr_t freeptr;
  if (fread(&freeptr, MDB_PTR_SIZE, 1, db->fp_index) != MDB_PTR_SIZE) {
    return mdb_status(MDB_ERR_READ, "cannot read free ptr");
  }

  if (freeptr != 0) {
    *ptr = freeptr;
    return mdb_status(MDB_OK, NULL);
  } else {
    return mdb_status(MDB_ERR_UNIMPLEMENTED, NULL);
  }
}

static mdb_status_t mdb_data_alloc(mdb_int_t *db, mdb_size_t valsize,
                                   mdb_ptr_t *ptr) {
  (void)db;
  (void)valsize;
  (void)ptr;
  return mdb_status(MDB_ERR_UNIMPLEMENTED, NULL);
}

static mdb_status_t mdb_index_free(mdb_int_t *db, mdb_ptr_t ptr) {
  if (fseek(db->fp_index, 0, SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to head of index file");
  }
  mdb_t freeptr;
  if (fread(&freeptr, MDB_PTR_SIZE, 1, db->fp_index) != MDB_PTR_SIZE) {
    return mdb_status(MDB_ERR_READ, "cannot read free ptr");
  }

  if (fseek(db->fp_index, 0, SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to head of index file");
  }
  if (fwrite(&ptr, MDB_PTR_SIZE, 1, db->fp_index)) {
    return mdb_status(MDB_ERR_WRITE, "cannot write to head of index file");
  }

  if (fseek(db->fp_index, (long)ptr, SEEK_SET) != 0) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to ptr");
  }

  if (fwrite(&freeptr, MDB_PTR_SIZE, 1, db->fp_index) < MDB_PTR_SIZE) {
    return mdb_status(MDB_ERR_WRITE, "cannot write to ptr");
  }

  return mdb_status(MDB_OK, NULL);
}

static mdb_status_t mdb_data_free(mdb_int_t *db, mdb_ptr_t valptr,
                                  mdb_size_t valsize) {
  if (fseek(db->fp_data, (long)valptr, SEEK_SET)) {
    return mdb_status(MDB_ERR_SEEK, "cannot seek to data record");
  }
  if (fwrite("", 1, valsize, db->fp_data) != valsize) {
    return mdb_status(MDB_ERR_WRITE, "cannot write empty data");
  }
  return mdb_status(MDB_OK, NULL);
}

static mdb_status_t mdb_status(uint8_t code, const char *desc) {
  mdb_status_t s;
  s.code = code;
  s.desc = desc;
  return s;
}

static mdb_int_t *mdb_alloc() {
  mdb_int_t *ret = (mdb_int_t*)malloc(sizeof(mdb_int_t));
  if (!ret) {
    return NULL;
  }
  ret->db_name = (char*)malloc(DB_NAME_MAX + 1);
  if (!ret->db_name) {
    free(ret);
    return NULL;
  }
  ret->options.db_name = ret->db_name;
  return ret;
}

static void mdb_free(mdb_int_t *db) {
  if (db->fp_superblock != NULL) {
    fclose(db->fp_superblock);
  }
  if (db->fp_index != NULL) {
    fclose(db->fp_index);
  }
  if (db->fp_data != NULL) {
    fclose(db->fp_data);
  }
  free(db->db_name);
  free(db);
}

static uint32_t mdb_hash(const char *key) {
  uint32_t ret = 0;
  for (uint32_t i = 0; *key != '\0'; ++key, ++i) {
    ret += (uint32_t)*key * i;
  }
  return ret;
}
