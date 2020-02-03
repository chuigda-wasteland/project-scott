#include "mdb.c"
#include "vktest.h"

#include <time.h>

#define TESTDB_DB_NAME "testdb"
#define TESTDB_KEY_SIZE_MAX 8
#define TESTDB_DATA_SIZE_MAX 1024
#define TESTDB_HASH_BUCKETS 512
#define TESTDB_ITEMS_MAX 65536

mdb_options_t get_default_options() {
  mdb_options_t ret;
  ret.db_name = TESTDB_DB_NAME;
  ret.key_size_max = TESTDB_KEY_SIZE_MAX;
  ret.data_size_max = TESTDB_DATA_SIZE_MAX;
  ret.hash_buckets = TESTDB_HASH_BUCKETS;
  ret.items_max = TESTDB_ITEMS_MAX;
  return ret;
}

mdb_options_t get_options_no_hash_buckets() {
  mdb_options_t ret = get_default_options();
  ret.hash_buckets = 0;
  return ret;
}

mdb_int_t open_test_db(mdb_options_t options) {
  mdb_int_t ret;
  ret.options = options;
  ret.fp_superblock = fopen("super", "wb+");
  ret.fp_index = fopen("index", "wb+");
  mdb_ptr_t free_ptr = 0;
  fwrite(&free_ptr, MDB_PTR_SIZE, 1, ret.fp_index);
  ret.fp_data = fopen("data", "wb+");

  ret.index_record_size = ret.options.key_size_max
                          + MDB_PTR_SIZE * 2
                          + MDB_DATALEN_SIZE;
  return ret;
}

void close_test_db(mdb_int_t db) {
  fclose(db.fp_superblock);
  fclose(db.fp_index);
  fclose(db.fp_data);
}

void generate_random_key(char *buffer) {
  for (size_t i = 0; i < TESTDB_KEY_SIZE_MAX - 1; i++) {
    buffer[i] = rand() % 128;
  }
  buffer[TESTDB_KEY_SIZE_MAX - 1] = 0;
}

void generate_seq_key(char *buffer, size_t seq) {
  memset(buffer, seq, TESTDB_KEY_SIZE_MAX - 1);
  buffer[TESTDB_KEY_SIZE_MAX] = 0;
}

void test1() {
  VK_TEST_SECTION_BEGIN("simple index write");

  mdb_int_t testdb = open_test_db(get_options_no_hash_buckets());
  mdb_ptr_t idxptr_arr[32];
  static char keys[32][TESTDB_KEY_SIZE_MAX];
  mdb_ptr_t valptr_arr[32];
  mdb_size_t valsize_arr[32];

  for (size_t i = 0; i < 32; i++) {
    generate_seq_key(keys[i], i);
    valptr_arr[i] = rand();
    valsize_arr[i] = rand();
    mdb_status_t index_alloc_status = mdb_index_alloc(&testdb, idxptr_arr + i);
    mdb_status_t index_write_status = mdb_write_index(&testdb, idxptr_arr[i],
                                                      keys[i], valptr_arr[i],
                                                      valsize_arr[i]);
    VK_ASSERT_EQUALS(MDB_OK, index_alloc_status.code);
    VK_ASSERT_EQUALS(MDB_OK, index_write_status.code);
  }

  for (size_t i = 0; i < 32; i++) {
    char test_key[TESTDB_KEY_SIZE_MAX + 1];
    mdb_ptr_t test_nextptr;
    mdb_ptr_t test_valptr;
    mdb_size_t test_valsize;

    mdb_status_t index_read_status = mdb_read_index(&testdb, idxptr_arr[i],
                                                   &test_nextptr,
                                                   test_key,
                                                   &test_valptr,
                                                   &test_valsize);
    fprintf(stderr, "i = %d\n", i);
    VK_ASSERT_EQUALS(MDB_OK, index_read_status.code);

    VK_ASSERT_EQUALS_S(keys[i], test_key);
    VK_ASSERT_EQUALS(valptr_arr[i], test_valptr);
    VK_ASSERT_EQUALS(valsize_arr[i], test_valsize);
  }

  VK_TEST_SECTION_END("simple index write");
}

int main() {
  srand(time(NULL));
  test1();
  return 0;
}
