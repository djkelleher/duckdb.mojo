#include "duckdb.h"
#include <stdio.h>

void write_table()
{
    duckdb_database db;
    duckdb_connection con;

    if (duckdb_open("test.db", &db) == DuckDBError)
    {
        printf("ERROR opening database\n");
    }
    if (duckdb_connect(db, &con) == DuckDBError)
    {
        printf("ERROR connecting to database\n");
    }
    printf("writing table\n");
    duckdb_state state;
    state = duckdb_query(con, "CREATE TABLE test_tbl (int_col INTEGER, float_col FLOAT, text_col TEXT)", NULL);
    if (state == DuckDBError)
    {
        printf("ERROR creating table\n");
        return;
    }
    int rows = 0;
    while (rows++ < 10000)
    {
        state = duckdb_query(con, "INSERT INTO test_tbl VALUES (4321, 1234.0, 'ducks')", NULL);
        if (state == DuckDBError)
        {
            printf("ERROR inserting row\n");
        }
    }
    duckdb_disconnect(&con);
    duckdb_close(&db);
}

void read_table()
{
    duckdb_database db;
    duckdb_connection con;

    if (duckdb_open("test.db", &db) == DuckDBError)
    {
        printf("ERROR opening database\n");
    }
    if (duckdb_connect(db, &con) == DuckDBError)
    {
        printf("ERROR connecting to database\n");
    }
    duckdb_state state;
    duckdb_result result;

    // create a table
    state = duckdb_query(con, "SELECT * FROM test_tbl", &result);
    if (state == DuckDBError)
    {
        printf("ERROR executing query\n");
    }

    idx_t vec_sz = duckdb_vector_size();
    printf("VECTOR SIZE: %li\n", vec_sz);

    idx_t row_count = duckdb_row_count(&result);
    printf("ROW COUNT: %li\n", row_count);

    bool is_streaming = duckdb_result_is_streaming(result);
    printf("IS STREAMING: %d\n", is_streaming);

    int chunk_count = duckdb_result_chunk_count(result);
    printf("CHUNK COUNT: %d\n", chunk_count);

    duckdb_data_chunk chunk = duckdb_result_get_chunk(result, 1);

    idx_t chunk_row_count = duckdb_data_chunk_get_size(chunk);
    printf("CHUNK ROW COUNT: %li\n", chunk_row_count);

    idx_t col_cnt = duckdb_data_chunk_get_column_count(chunk);
    printf("CHUNK COLUMN COUNT: %li\n", col_cnt);

    for (idx_t c = 0; c < col_cnt; c++)
    {
        printf("CHUNK COL: %ld\n", c);
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, c);
        duckdb_logical_type type = duckdb_vector_get_column_type(vec);
        void *data = duckdb_vector_get_data(vec);
        uint64_t *validity = duckdb_vector_get_validity(vec);
    }
    duckdb_destroy_data_chunk(&chunk);
    duckdb_destroy_result(&result);
    duckdb_disconnect(&con);
    duckdb_close(&db);
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++)
    {
        char *arg = argv[i];
        if (*arg == 'r')
        {
            read_table();
        }
        else if (*arg == 'w')
        {
            write_table();
        }
    }
}
