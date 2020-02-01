#include "mdb.c"
#include "vktest.h"

mdb_options_t get_default_options() {
  mdb_options_t ret;
  ret.db_name = "testdb";
  ret.key_size_max = 128;
  ret.data_size_max = 1024;
  ret.hash_buckets = 512;
  ret.items_max = 65536;
  return ret;
}

mdb_options_t get_options_nohash() {
  mdb_options_t ret = get_default_options();
  ret.hash_buckets = 0;
  return ret;
}

mdb_int_t open_test_db(mdb_options_t options) {
  mdb_int_t ret;
  ret.options = options;
  ret.fp_superblock = fopen("super", "w");
  ret.fp_index = fopen("index", "w");
  ret.fp_data = fopen("data", "w");
  return ret;
}

void close_test_db(mdb_int_t db) {
  fclose(db->fp_superblock);
  fclose(db->fp_index);
  fclose(db->fp_data);
}

void test1() {
  VK_TEST_SECTION_BEGIN("simple index write");

  

  Vk_TEST_SECTION_END();
}
