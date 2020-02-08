#include "vktest.h"

#include "mdb.h"

void happy_test0() {
  mdb_options_t options;
  options.db_name = "misakawa";
  options.key_size_max = 64;
  options.data_size_max = 256;
  options.hash_buckets = 128;
  options.items_max = 166716;

  mdb_t db;
  mdb_status_t db_create_status = mdb_create(&db, options);
  if (db_create_status.code != MDB_OK) {
    fprintf(stderr, "Error creating database: E%d: %s\n",
            db_create_status.code,
            db_create_status.desc ? db_create_status.desc : "(NULL)");
    abort();
  }

  mdb_status_t db_write_status = mdb_write(db, "misakawa", "mikoto");
  if (db_write_status.code != MDB_OK) {
    fprintf(stderr, "Error writing into database: E%d: %s\n",
            db_write_status.code,
            db_write_status.desc ? db_create_status.desc : "(NULL)");
    abort();
  }

  char buffer[257];
  mdb_status_t db_read_status = mdb_read(db, "misakawa", buffer, 257);
  if (db_read_status.code != MDB_OK) {
    fprintf(stderr, "Error reading from database: E%d: %s\n",
            db_read_status.code,
            db_read_status.desc ? db_create_status.desc : "(NULL)");
    abort();
  }

  fprintf(stderr, "\"misakawa\" => \"%s\"\n", buffer);

  mdb_close(db);
}

int main() {
  happy_test0();
}
